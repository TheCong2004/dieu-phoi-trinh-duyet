use super::worker_types::*;
use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;
use log::{error, info, warn};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

lazy_static! {
  pub static ref WORKER_REGISTRY: WorkerRegistry = WorkerRegistry::new();
}

#[derive(Default)]
struct RegistryState {
  workers: HashMap<String, BrowserWorker>,
  leases: HashMap<String, WorkerLease>,
}

#[derive(Clone)]
pub struct WorkerRegistry {
  state: Arc<Mutex<RegistryState>>,
  storage_path: Option<PathBuf>,
}

impl Default for WorkerRegistry {
  fn default() -> Self {
    Self::new()
  }
}

impl WorkerRegistry {
  /// Production constructor: Starts with EMPTY in-memory maps.
  /// Workers are populated strictly from real runtime probes / profile manager.
  pub fn new() -> Self {
    let storage_path = Some(crate::app_dirs::data_dir().join("worker_leases.json"));
    Self {
      state: Arc::new(Mutex::new(RegistryState::default())),
      storage_path,
    }
  }

  /// Constructor for tests with custom in-memory or temporary storage path.
  pub fn with_custom_storage(storage_path: Option<PathBuf>) -> Self {
    Self {
      state: Arc::new(Mutex::new(RegistryState::default())),
      storage_path,
    }
  }

  /// Internal helper to persist all durable leases to disk.
  fn persist_leases_to_disk(&self, leases: &HashMap<String, WorkerLease>) {
    if let Some(ref path) = self.storage_path {
      if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
      }
      let lease_list: Vec<&WorkerLease> = leases.values().collect();
      if let Ok(json) = serde_json::to_string_pretty(&lease_list) {
        let _ = fs::write(path, json);
      }
    }
  }

  /// Register or update a browser worker based on real runtime / extension health handshake.
  pub async fn register_or_update_worker(&self, worker: BrowserWorker) -> Result<(), WorkerError> {
    let mut state = self.state.lock().await;

    // Validate protocol version if extension is connected
    if let Some(proto) = worker.protocol_version {
      if proto != 1 {
        return Err(WorkerError::new(
          WorkerErrorCode::ProtocolMismatch,
          format!("Incompatible extension protocol version {proto}"),
        ));
      }
    }

    state.workers.insert(worker.worker_id.clone(), worker);
    Ok(())
  }

  /// Real worker handshake from browser runtime / extension health probe.
  pub async fn handle_health_handshake(
    &self,
    worker_id: &str,
    req: WorkerHealthHandshakeRequest,
  ) -> Result<(), WorkerError> {
    let mut state = self.state.lock().await;

    if req.protocol_version != 1 {
      return Err(WorkerError::new(
        WorkerErrorCode::ProtocolMismatch,
        format!("Protocol version mismatch: expected 1, got {}", req.protocol_version),
      ));
    }

    let worker = state.workers.get_mut(worker_id).ok_or_else(|| {
      WorkerError::new(WorkerErrorCode::NoAvailableWorker, format!("Worker {worker_id} not registered in runtime"))
    })?;

    if worker.profile_id != req.profile_id {
      worker.state = WorkerState::Error;
      return Err(WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!(
          "Health profileId ({}) does not match runtime profileId ({})",
          req.profile_id, worker.profile_id
        ),
      ));
    }

    worker.extension_ready = true;
    worker.extension_version = Some(req.extension_version);
    worker.protocol_version = Some(req.protocol_version);
    worker.grok_logged_in = Some(req.logged_in);
    worker.capabilities = req.capabilities;
    worker.last_heartbeat_at = Some(Utc::now().to_rfc3339());

    if !req.logged_in {
      worker.state = WorkerState::LoginRequired;
    } else if req.worker_state == "BUSY" || req.worker_state == "LEASED" {
      worker.state = WorkerState::Busy;
    } else if worker.current_lease_id.is_none() {
      worker.state = WorkerState::Ready;
    }

    Ok(())
  }

  /// Atomically acquires an exclusive worker lease for a specific step attempt.
  pub async fn acquire(&self, req: AcquireWorkerRequest) -> Result<AcquireWorkerResponse, WorkerError> {
    let mut state = self.state.lock().await;
    let now = Utc::now();

    // 1. First reap expired leases safely (marks worker RECONCILING, never blindly READY)
    let mut modified = false;
    for lease in state.leases.values_mut() {
      if lease.status == LeaseStatus::Active {
        if let Ok(exp) = DateTime::parse_from_rfc3339(&lease.expires_at) {
          if now > exp.with_timezone(&Utc) {
            lease.status = LeaseStatus::Expired;
            modified = true;
            if let Some(w) = state.workers.get_mut(&lease.worker_id) {
              if w.current_lease_id.as_deref() == Some(&lease.lease_id) {
                w.current_lease_id = None;
                w.current_job_id = None;
                // Move to RECONCILING, not immediately READY to prevent cross-job collision
                w.state = WorkerState::Reconciling;
              }
            }
          }
        }
      }
    }

    // 2. Validate pool filter if specified
    if let Some(ref pool) = req.pool_id {
      let pool_exists = state.workers.values().any(|w| w.pool_id.as_deref() == Some(pool.as_str()));
      if !pool_exists {
        return Err(WorkerError::new(
          WorkerErrorCode::PoolNotFound,
          format!("Worker pool '{pool}' does not exist"),
        ));
      }
    }

    // 3. Filter eligible ready workers matching pool, capability, health, and login status
    let eligible_worker_id = state
      .workers
      .values()
      .find(|w| {
        let pool_ok = match req.pool_id {
          Some(ref p) => w.pool_id.as_deref() == Some(p.as_str()),
          None => true,
        };

        pool_ok
          && w.state == WorkerState::Ready
          && w.current_lease_id.is_none()
          && w.extension_ready
          && w.grok_logged_in.unwrap_or(false)
          && w.capabilities.iter().any(|c| c == &req.capability)
      })
      .map(|w| w.worker_id.clone());

    let worker_id = match eligible_worker_id {
      Some(id) => id,
      None => {
        // Check why no worker is eligible
        let any_matching_cap = state.workers.values().any(|w| {
          let pool_ok = match req.pool_id {
            Some(ref p) => w.pool_id.as_deref() == Some(p.as_str()),
            None => true,
          };
          pool_ok && w.capabilities.iter().any(|c| c == &req.capability)
        });

        if any_matching_cap {
          return Err(WorkerError::new(
            WorkerErrorCode::WorkerBusy,
            "All eligible workers are currently leased, busy or reconciling",
          ));
        } else {
          return Err(WorkerError::new(
            WorkerErrorCode::NoAvailableWorker,
            "No worker found with requested capability",
          ));
        }
      }
    };

    let worker = state.workers.get_mut(&worker_id).unwrap();
    let lease_id = format!("LEASE_{}", Uuid::new_v4().simple());
    let ttl_secs = req.ttl_seconds.unwrap_or(120);
    let expires_at = (now + Duration::seconds(ttl_secs as i64)).to_rfc3339();

    worker.state = WorkerState::Leased;
    worker.current_lease_id = Some(lease_id.clone());
    worker.current_job_id = Some(req.job_id.clone());
    worker.last_heartbeat_at = Some(now.to_rfc3339());

    let lease = WorkerLease {
      lease_id: lease_id.clone(),
      worker_id: worker.worker_id.clone(),
      profile_id: worker.profile_id.clone(),
      job_id: req.job_id,
      step_id: req.step_id,
      attempt_id: req.attempt_id,
      capability: req.capability,
      status: LeaseStatus::Active,
      acquired_at: now.to_rfc3339(),
      expires_at: expires_at.clone(),
      last_heartbeat_at: now.to_rfc3339(),
    };

    state.leases.insert(lease_id.clone(), lease);
    self.persist_leases_to_disk(&state.leases);

    Ok(AcquireWorkerResponse {
      lease_id,
      worker_id: worker.worker_id.clone(),
      profile_id: worker.profile_id.clone(),
      expires_at,
    })
  }

  /// Renews lease expiration timestamp via heartbeat with strict correlation check.
  pub async fn heartbeat(&self, lease_id: &str, req: HeartbeatLeaseRequest) -> Result<HeartbeatLeaseResponse, WorkerError> {
    let mut state = self.state.lock().await;

    let lease = state
      .leases
      .get_mut(lease_id)
      .ok_or_else(|| WorkerError::new(WorkerErrorCode::LeaseNotFound, "Lease not found"))?;

    if lease.status != LeaseStatus::Active {
      return Err(WorkerError::new(
        WorkerErrorCode::LeaseNotActive,
        format!("Lease is not active: {:?}", lease.status),
      ));
    }

    if lease.job_id != req.job_id || lease.attempt_id != req.attempt_id {
      return Err(WorkerError::new(
        WorkerErrorCode::CorrelationMismatch,
        "Lease correlation mismatch for heartbeat",
      ));
    }

    let now = Utc::now();
    let ttl_secs = req.ttl_seconds.unwrap_or(60);
    let expires_at = (now + Duration::seconds(ttl_secs as i64)).to_rfc3339();

    lease.expires_at = expires_at.clone();
    lease.last_heartbeat_at = now.to_rfc3339();

    if let Some(w) = state.workers.get_mut(&lease.worker_id) {
      w.last_heartbeat_at = Some(now.to_rfc3339());
    }

    self.persist_leases_to_disk(&state.leases);

    Ok(HeartbeatLeaseResponse {
      lease_id: lease_id.to_string(),
      status: LeaseStatus::Active,
      expires_at,
    })
  }

  /// Safe release: Released lease moves worker to RECONCILING (NOT immediately READY).
  pub async fn release(&self, lease_id: &str) -> ReleaseLeaseResponse {
    let mut state = self.state.lock().await;

    if let Some(lease) = state.leases.get_mut(lease_id) {
      lease.status = LeaseStatus::Released;
      if let Some(w) = state.workers.get_mut(&lease.worker_id) {
        if w.current_lease_id.as_deref() == Some(lease_id) {
          w.current_lease_id = None;
          w.current_job_id = None;
          // Crucial safety rule: RELEASED LEASE != READY WORKER.
          // Worker transitions to RECONCILING until health probe confirms IDLE.
          w.state = WorkerState::Reconciling;
        }
      }
    }

    self.persist_leases_to_disk(&state.leases);

    ReleaseLeaseResponse {
      lease_id: lease_id.to_string(),
      status: LeaseStatus::Released,
    }
  }

  /// Explicit worker reconciliation: Called after health probe confirms worker state.
  pub async fn reconcile_worker(
    &self,
    worker_id: &str,
    is_idle: bool,
    is_healthy: bool,
    grok_logged_in: Option<bool>,
  ) -> Result<WorkerState, WorkerError> {
    let mut state = self.state.lock().await;

    let worker = state
      .workers
      .get_mut(worker_id)
      .ok_or_else(|| WorkerError::new(WorkerErrorCode::NoAvailableWorker, format!("Worker {worker_id} not found")))?;

    if !is_healthy {
      worker.state = WorkerState::Error;
      return Ok(WorkerState::Error);
    }

    if let Some(logged_in) = grok_logged_in {
      worker.grok_logged_in = Some(logged_in);
      if !logged_in {
        worker.state = WorkerState::LoginRequired;
        return Ok(WorkerState::LoginRequired);
      }
    }

    if !is_idle {
      worker.state = WorkerState::Busy;
      return Ok(WorkerState::Busy);
    }

    // Only transition to READY if no active lease is held
    if worker.current_lease_id.is_none() && worker.grok_logged_in.unwrap_or(false) && worker.extension_ready {
      worker.state = WorkerState::Ready;
    } else if worker.current_lease_id.is_some() {
      worker.state = WorkerState::Leased;
    }

    Ok(worker.state.clone())
  }

  /// Reconcile and recover active leases from persistent store after daemon restart.
  pub async fn recover_leases(&self, durable_leases: Vec<WorkerLease>) {
    let mut state = self.state.lock().await;
    let now = Utc::now();

    for lease in durable_leases {
      if lease.status == LeaseStatus::Active {
        // Check if already expired
        let is_expired = if let Ok(exp) = DateTime::parse_from_rfc3339(&lease.expires_at) {
          now > exp.with_timezone(&Utc)
        } else {
          true
        };

        if is_expired {
          let mut expired_lease = lease.clone();
          expired_lease.status = LeaseStatus::Expired;
          state.leases.insert(lease.lease_id.clone(), expired_lease);
          if let Some(w) = state.workers.get_mut(&lease.worker_id) {
            w.state = WorkerState::Reconciling;
          }
        } else {
          // Recover active lease ownership - DO NOT make available to other jobs
          state.leases.insert(lease.lease_id.clone(), lease.clone());
          if let Some(w) = state.workers.get_mut(&lease.worker_id) {
            w.current_lease_id = Some(lease.lease_id.clone());
            w.current_job_id = Some(lease.job_id.clone());
            w.state = WorkerState::Leased;
          }
        }
      } else {
        state.leases.insert(lease.lease_id.clone(), lease);
      }
    }
  }

  /// Load durable leases from storage file if it exists.
  pub async fn load_from_storage(&self) {
    if let Some(ref path) = self.storage_path {
      if path.exists() {
        if let Ok(content) = fs::read_to_string(path) {
          if let Ok(leases) = serde_json::from_str::<Vec<WorkerLease>>(&content) {
            self.recover_leases(leases).await;
          }
        }
      }
    }
  }

  pub async fn list_workers(&self) -> ListWorkersResponse {
    let state = self.state.lock().await;
    let list: Vec<BrowserWorker> = state.workers.values().cloned().collect();
    let total = list.len();
    ListWorkersResponse { workers: list, total }
  }

  pub async fn list_leases(&self) -> ListLeasesResponse {
    let state = self.state.lock().await;
    let list: Vec<WorkerLease> = state.leases.values().cloned().collect();
    let total = list.len();
    ListLeasesResponse { leases: list, total }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn seed_test_worker(registry: &WorkerRegistry, worker_id: &str, profile_id: &str, pool_id: Option<&str>) {
    let worker = BrowserWorker {
      worker_id: worker_id.to_string(),
      profile_id: profile_id.to_string(),
      pool_id: pool_id.map(|s| s.to_string()),
      state: WorkerState::Ready,
      capabilities: vec![
        "grok.image.edit".to_string(),
        "grok.image.expand_9_16".to_string(),
      ],
      extension_ready: true,
      extension_version: Some("1.1.49".to_string()),
      protocol_version: Some(1),
      grok_logged_in: Some(true),
      current_lease_id: None,
      current_job_id: None,
      last_heartbeat_at: Some(Utc::now().to_rfc3339()),
      last_error: None,
    };
    let mut state = registry.state.blocking_lock();
    state.workers.insert(worker_id.to_string(), worker);
  }

  #[test]
  fn test_01_production_new_does_not_seed_fake_workers() {
    let registry = WorkerRegistry::new();
    let state = registry.state.blocking_lock();
    assert_eq!(state.workers.len(), 0, "Production WorkerRegistry::new() must start empty");
  }

  #[tokio::test]
  async fn test_02_health_handshake_registration() {
    let registry = WorkerRegistry::new();
    let worker = BrowserWorker {
      worker_id: "WORKER_01".to_string(),
      profile_id: "PROFILE_GROK_01".to_string(),
      pool_id: None,
      state: WorkerState::Starting,
      capabilities: vec![],
      extension_ready: false,
      extension_version: None,
      protocol_version: None,
      grok_logged_in: None,
      current_lease_id: None,
      current_job_id: None,
      last_heartbeat_at: None,
      last_error: None,
    };
    registry.register_or_update_worker(worker).await.unwrap();

    // Valid handshake
    registry.handle_health_handshake("WORKER_01", WorkerHealthHandshakeRequest {
      profile_id: "PROFILE_GROK_01".to_string(),
      protocol_version: 1,
      extension_version: "1.1.49".to_string(),
      worker_state: "IDLE".to_string(),
      logged_in: true,
      capabilities: vec!["grok.image.edit".to_string()],
    }).await.unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(w.state, WorkerState::Ready);
    assert_eq!(w.grok_logged_in, Some(true));
  }

  #[tokio::test]
  async fn test_03_safe_release_moves_worker_to_reconciling() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    }).await.unwrap();

    // Release lease
    registry.release(&res.lease_id).await;

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(w.state, WorkerState::Reconciling, "Released worker must be RECONCILING, not immediately READY");
  }

  #[tokio::test]
  async fn test_04_reconcile_idle_transitions_to_ready() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    }).await.unwrap();

    registry.release(&res.lease_id).await;

    // Reconcile confirming idle
    let st = registry.reconcile_worker("WORKER_01", true, true, Some(true)).await.unwrap();
    assert_eq!(st, WorkerState::Ready);
  }

  #[tokio::test]
  async fn test_05_reconcile_busy_remains_non_schedulable() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    }).await.unwrap();

    registry.release(&res.lease_id).await;

    // Reconcile reporting Extension is still busy
    let st = registry.reconcile_worker("WORKER_01", false, true, Some(true)).await.unwrap();
    assert_eq!(st, WorkerState::Busy);

    // Cannot acquire busy worker
    let err = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_002".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    }).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_06_typed_pool_not_found_error() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", Some("REAL_POOL"));

    let err = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: Some("UNKNOWN_POOL".to_string()),
      ttl_seconds: Some(120),
    }).await;

    assert!(err.is_err());
    let e = err.unwrap_err();
    assert_eq!(e.code, WorkerErrorCode::PoolNotFound);
    assert_eq!(e.status_code(), 404);
    assert_eq!(e.code_str(), "POOL_NOT_FOUND");
  }

  #[tokio::test]
  async fn test_07_restart_recovery_prevents_double_lease() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let active_lease = WorkerLease {
      lease_id: "LEASE_PERSISTED_001".to_string(),
      worker_id: "WORKER_01".to_string(),
      profile_id: "PROFILE_GROK_01".to_string(),
      job_id: "JOB_PREV".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      status: LeaseStatus::Active,
      acquired_at: Utc::now().to_rfc3339(),
      expires_at: (Utc::now() + Duration::seconds(120)).to_rfc3339(),
      last_heartbeat_at: Utc::now().to_rfc3339(),
    };

    // Recover durable leases
    registry.recover_leases(vec![active_lease]).await;

    // Try to acquire same worker for another job -> MUST FAIL
    let err = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_NEW".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    }).await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_08_100_acquire_release_reconcile_cycles_no_deadlock() {
    let registry = Arc::new(WorkerRegistry::new());
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    for i in 0..100 {
      let res = registry.acquire(AcquireWorkerRequest {
        job_id: format!("JOB_{i}"),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        ttl_seconds: Some(120),
      }).await.unwrap();

      registry.release(&res.lease_id).await;
      registry.reconcile_worker("WORKER_01", true, true, Some(true)).await.unwrap();
    }
  }
}
