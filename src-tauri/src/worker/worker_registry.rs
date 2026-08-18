use super::worker_types::*;
use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;
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
  is_ready: bool,
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
  /// Production constructor: Starts with EMPTY in-memory maps and is_ready = false.
  /// Workers are populated strictly from real runtime probes / profile manager.
  pub fn new() -> Self {
    let storage_path = Some(crate::app_dirs::data_dir().join("worker_leases.json"));
    Self {
      state: Arc::new(Mutex::new(RegistryState {
        workers: HashMap::new(),
        leases: HashMap::new(),
        is_ready: false,
      })),
      storage_path,
    }
  }

  /// Constructor for tests with custom in-memory or temporary storage path.
  pub fn with_custom_storage(storage_path: Option<PathBuf>) -> Self {
    Self {
      state: Arc::new(Mutex::new(RegistryState {
        workers: HashMap::new(),
        leases: HashMap::new(),
        is_ready: true,
      })),
      storage_path,
    }
  }

  /// Mark registry ready once startup recovery and profile sync finish.
  pub async fn mark_ready(&self) {
    let mut state = self.state.lock().await;
    state.is_ready = true;
  }

  /// Set readiness explicitly (used in tests).
  pub async fn set_ready(&self, ready: bool) {
    let mut state = self.state.lock().await;
    state.is_ready = ready;
  }

  /// Check whether registry is ready to serve acquire requests.
  pub async fn is_ready(&self) -> bool {
    let state = self.state.lock().await;
    state.is_ready
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

  /// Register or update a browser worker based on real runtime profile lifecycle.
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

  /// Mark a worker offline when its associated browser process is killed/closed.
  pub async fn mark_worker_offline(&self, worker_id: &str) -> Result<(), WorkerError> {
    let mut state = self.state.lock().await;
    let target_key = if state.workers.contains_key(worker_id) {
      Some(worker_id.to_string())
    } else {
      state
        .workers
        .values()
        .find(|w| w.profile_id == worker_id)
        .map(|w| w.worker_id.clone())
    };
    if let Some(key) = target_key {
      if let Some(worker) = state.workers.get_mut(&key) {
        worker.state = WorkerState::Offline;
        worker.extension_ready = false;
      }
    }
    Ok(())
  }

  /// Mark a worker as fatal error upon non-transient mismatch.
  pub async fn mark_worker_error(
    &self,
    worker_id: &str,
    error_msg: String,
  ) -> Result<(), WorkerError> {
    let mut state = self.state.lock().await;
    let target_key = if state.workers.contains_key(worker_id) {
      Some(worker_id.to_string())
    } else {
      state
        .workers
        .values()
        .find(|w| w.profile_id == worker_id)
        .map(|w| w.worker_id.clone())
    };
    if let Some(key) = target_key {
      if let Some(worker) = state.workers.get_mut(&key) {
        worker.state = WorkerState::Error;
        worker.last_error = Some(error_msg);
      }
    }
    Ok(())
  }

  /// Sync known profiles at daemon startup.
  pub async fn sync_startup_profiles(&self, profiles: &[crate::profile::types::BrowserProfile]) {
    let mut state = self.state.lock().await;
    for profile in profiles {
      let worker_id = format!("browser-profile:{}", profile.id);
      let is_running = profile.process_id.is_some();
      let default_state = if is_running {
        WorkerState::Reconciling
      } else {
        WorkerState::Offline
      };

      // Check if there is an existing active recovered lease for this profile
      let existing_lease = state
        .leases
        .values()
        .find(|l| l.profile_id == profile.id.to_string() && l.status == LeaseStatus::Active)
        .cloned();

      let worker = state
        .workers
        .entry(worker_id.clone())
        .or_insert_with(|| BrowserWorker {
          worker_id: worker_id.clone(),
          profile_id: profile.id.to_string(),
          pool_id: profile.group_id.clone(),
          state: default_state,
          capabilities: vec![],
          extension_ready: false,
          extension_version: None,
          protocol_version: None,
          grok_logged_in: None,
          current_lease_id: None,
          current_job_id: None,
          last_heartbeat_at: None,
          last_error: None,
        });

      if let Some(ref lease) = existing_lease {
        worker.current_lease_id = Some(lease.lease_id.clone());
        worker.current_job_id = Some(lease.job_id.clone());
        worker.state = WorkerState::Leased;
      }
    }
  }

  /// Real worker handshake from browser runtime / extension health probe.
  /// Seamlessly performs health update and automatic reconciliation.
  pub async fn handle_health_handshake(
    &self,
    worker_id: &str,
    req: WorkerHealthHandshakeRequest,
  ) -> Result<(), WorkerError> {
    let mut state = self.state.lock().await;

    if req.protocol_version != 1 {
      return Err(WorkerError::new(
        WorkerErrorCode::ProtocolMismatch,
        format!(
          "Protocol version mismatch: expected 1, got {}",
          req.protocol_version
        ),
      ));
    }

    let target_key = if state.workers.contains_key(worker_id) {
      Some(worker_id.to_string())
    } else {
      state
        .workers
        .values()
        .find(|w| w.profile_id == worker_id || w.profile_id == req.profile_id)
        .map(|w| w.worker_id.clone())
    };

    let target_key = target_key.ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::NoAvailableWorker,
        format!("Worker {worker_id} not registered in runtime"),
      )
    })?;

    let worker = state.workers.get_mut(&target_key).unwrap();

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
    } else if worker.current_lease_id.is_some() {
      // Active lease authority wins: Worker remains Leased/Busy regardless of whether extension reports IDLE or BUSY
      worker.state = if req.worker_state == "BUSY" {
        WorkerState::Busy
      } else {
        WorkerState::Leased
      };
    } else if req.worker_state == "BUSY" || req.worker_state == "LEASED" {
      worker.state = WorkerState::Busy;
    } else if req.worker_state == "IDLE" {
      // Safe Automatic Reconciliation: Transition to Ready ONLY when extension is strictly IDLE and has NO active lease
      worker.state = WorkerState::Ready;
    } else if req.worker_state == "STARTING" || req.worker_state == "RECONCILING" {
      worker.state = WorkerState::Reconciling;
    } else {
      worker.state = WorkerState::Error;
    }

    Ok(())
  }

  /// Reconciles a worker's state upon health probe or background monitor.
  /// Transient failures keep the worker in Reconciling so background probes continue retrying.
  pub async fn reconcile_worker(
    &self,
    worker_id: &str,
    is_idle: bool,
    is_healthy: bool,
    grok_logged_in: Option<bool>,
  ) -> Result<WorkerState, WorkerError> {
    let mut state = self.state.lock().await;

    let target_key = if state.workers.contains_key(worker_id) {
      Some(worker_id.to_string())
    } else {
      state
        .workers
        .values()
        .find(|w| w.profile_id == worker_id)
        .map(|w| w.worker_id.clone())
    };

    let target_key = target_key.ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::NoAvailableWorker,
        format!("Worker '{worker_id}' not found in registry"),
      )
    })?;

    let worker = state.workers.get_mut(&target_key).unwrap();

    if let Some(logged_in) = grok_logged_in {
      worker.grok_logged_in = Some(logged_in);
    }

    if !is_healthy {
      // Transient failure -> Move to Reconciling (NOT permanent Error) so background loop retries
      worker.state = WorkerState::Reconciling;
      return Ok(WorkerState::Reconciling);
    }

    if worker.current_lease_id.is_some() {
      // Active lease wins -> remain Leased/Busy
      worker.state = if is_idle {
        WorkerState::Leased
      } else {
        WorkerState::Busy
      };
    } else if worker.grok_logged_in == Some(false) {
      worker.state = WorkerState::LoginRequired;
    } else if is_idle {
      worker.state = WorkerState::Ready;
    } else {
      worker.state = WorkerState::Busy;
    }

    Ok(worker.state.clone())
  }

  /// Atomically acquires an exclusive worker lease for a specific step attempt.
  pub async fn acquire(
    &self,
    req: AcquireWorkerRequest,
  ) -> Result<AcquireWorkerResponse, WorkerError> {
    let mut state = self.state.lock().await;

    // Startup Readiness Barrier: Reject with 503 if still initializing
    if !state.is_ready {
      return Err(WorkerError::new(
        WorkerErrorCode::WorkerRegistryInitializing,
        "Worker registry is still initializing startup recovery",
      ));
    }

    let now = Utc::now();

    // 1. First reap expired leases safely (marks worker RECONCILING, never blindly READY)
    let mut expired_workers = Vec::new();
    for lease in state.leases.values_mut() {
      if lease.status == LeaseStatus::Active {
        if let Ok(exp) = DateTime::parse_from_rfc3339(&lease.expires_at) {
          if now > exp.with_timezone(&Utc) {
            lease.status = LeaseStatus::Expired;
            expired_workers.push((lease.lease_id.clone(), lease.worker_id.clone()));
          }
        }
      }
    }
    for (lease_id, worker_id) in expired_workers {
      if let Some(w) = state.workers.get_mut(&worker_id) {
        if w.current_lease_id.as_deref() == Some(&lease_id) {
          w.current_lease_id = None;
          w.current_job_id = None;
          w.state = WorkerState::Reconciling;
        }
      }
    }

    // 2. Validate pool filter if specified
    if let Some(ref pool) = req.pool_id {
      let pool_exists = state
        .workers
        .values()
        .any(|w| w.pool_id.as_deref() == Some(pool.as_str()));
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
        // State must be READY
        if w.state != WorkerState::Ready {
          return false;
        }
        // Pool filter
        if let Some(ref pool) = req.pool_id {
          if w.pool_id.as_deref() != Some(pool.as_str()) {
            return false;
          }
        }
        // Capability filter
        if !w.capabilities.contains(&req.capability) {
          return false;
        }
        // Extension must be confirmed ready
        if !w.extension_ready {
          return false;
        }
        // Must be logged into Grok
        if w.grok_logged_in != Some(true) {
          return false;
        }
        // Must not hold an existing lease
        if w.current_lease_id.is_some() {
          return false;
        }
        true
      })
      .map(|w| w.worker_id.clone());

    // Strict 1:1 Profile Pinning: If profile_id is provided, match exact profile only
    let worker_id = if let Some(ref pid) = req.profile_id {
      state
        .workers
        .values()
        .find(|w| {
          &w.profile_id == pid
            && w.state == WorkerState::Ready
            && w.extension_ready
            && w.grok_logged_in == Some(true)
            && w.current_lease_id.is_none()
        })
        .map(|w| w.worker_id.clone())
        .ok_or_else(|| {
          WorkerError::new(
            WorkerErrorCode::NoAvailableWorker,
            format!("Specified profile '{pid}' is not ready or does not exist"),
          )
        })?
    } else {
      match eligible_worker_id {
        Some(id) => id,
        None => {
          // Check if any worker exists with this pool to provide accurate error code
          if let Some(ref pool) = req.pool_id {
            let pool_workers = state
              .workers
              .values()
              .filter(|w| w.pool_id.as_deref() == Some(pool.as_str()))
              .collect::<Vec<_>>();
            if pool_workers
              .iter()
              .any(|w| w.state == WorkerState::LoginRequired || w.grok_logged_in == Some(false))
            {
              return Err(WorkerError::new(
                WorkerErrorCode::GrokNotLoggedIn,
                "Worker in pool requires Grok login",
              ));
            }
          }
          // Check if matching capability workers exist but are currently busy/leased/reconciling
          let matching_cap_busy = state.workers.values().any(|w| {
            w.capabilities.contains(&req.capability)
              && (w.state == WorkerState::Busy
                || w.state == WorkerState::Leased
                || w.state == WorkerState::Reconciling
                || w.current_lease_id.is_some())
          });
          if matching_cap_busy {
            return Err(WorkerError::new(
              WorkerErrorCode::WorkerBusy,
              "Matching workers are currently busy or leased",
            ));
          }
          return Err(WorkerError::new(
            WorkerErrorCode::NoAvailableWorker,
            "No ready workers available matching capability and pool requirements",
          ));
        }
      }
    };

    let lease_id = format!("LEASE_{}", Uuid::new_v4().simple());
    let ttl_secs = req.ttl_seconds.unwrap_or(120);
    let expires_at = (now + Duration::seconds(ttl_secs as i64)).to_rfc3339();

    let (worker_id_str, profile_id_str) = {
      let worker = state.workers.get_mut(&worker_id).unwrap();
      worker.state = WorkerState::Leased;
      worker.current_lease_id = Some(lease_id.clone());
      worker.current_job_id = Some(req.job_id.clone());
      worker.last_heartbeat_at = Some(now.to_rfc3339());
      (worker.worker_id.clone(), worker.profile_id.clone())
    };

    let lease = WorkerLease {
      lease_id: lease_id.clone(),
      worker_id: worker_id_str.clone(),
      profile_id: profile_id_str.clone(),
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
      worker_id: worker_id_str,
      profile_id: profile_id_str,
      expires_at,
    })
  }

  /// Renews lease expiration timestamp via heartbeat with strict correlation check.
  pub async fn heartbeat(
    &self,
    lease_id: &str,
    req: HeartbeatLeaseRequest,
  ) -> Result<HeartbeatLeaseResponse, WorkerError> {
    let mut state = self.state.lock().await;

    let now = Utc::now();
    let ttl_secs = req.ttl_seconds.unwrap_or(60);
    let expires_at = (now + Duration::seconds(ttl_secs as i64)).to_rfc3339();

    let worker_id = {
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

      lease.expires_at = expires_at.clone();
      lease.last_heartbeat_at = now.to_rfc3339();
      lease.worker_id.clone()
    };

    if let Some(w) = state.workers.get_mut(&worker_id) {
      w.last_heartbeat_at = Some(now.to_rfc3339());
    }

    self.persist_leases_to_disk(&state.leases);

    Ok(HeartbeatLeaseResponse {
      lease_id: lease_id.to_string(),
      status: LeaseStatus::Active,
      expires_at,
    })
  }

  /// Validates that an active lease exists and matches the exact correlation identity for dispatch.
  pub async fn validate_active_lease(
    &self,
    lease_id: &str,
    job_id: &str,
    step_id: &str,
    attempt_id: &str,
    profile_id: &str,
  ) -> Result<WorkerLease, WorkerError> {
    let state = self.state.lock().await;

    let lease = state.leases.get(lease_id).ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::LeaseNotFound,
        format!("Lease '{lease_id}' not found"),
      )
    })?;

    if lease.status != LeaseStatus::Active {
      return Err(WorkerError::new(
        WorkerErrorCode::LeaseNotActive,
        format!("Lease '{lease_id}' is not active: {:?}", lease.status),
      ));
    }

    if lease.job_id != job_id || lease.step_id != step_id || lease.attempt_id != attempt_id {
      return Err(WorkerError::new(
        WorkerErrorCode::CorrelationMismatch,
        format!(
          "Lease correlation mismatch. Expected job={}, step={}, attempt={}; got job={}, step={}, attempt={}",
          lease.job_id, lease.step_id, lease.attempt_id, job_id, step_id, attempt_id
        ),
      ));
    }

    if lease.profile_id != profile_id && lease.worker_id != profile_id {
      return Err(WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!(
          "Lease profile mismatch: lease profile={}, requested profile={}",
          lease.profile_id, profile_id
        ),
      ));
    }

    // 2. Authoritative Worker Record Cross-Check
    let worker = state
      .workers
      .get(&lease.worker_id)
      .or_else(|| state.workers.values().find(|w| w.profile_id == profile_id))
      .ok_or_else(|| {
        WorkerError::new(
          WorkerErrorCode::NoAvailableWorker,
          format!("Worker for lease '{lease_id}' not found in registry"),
        )
      })?;

    if worker.profile_id != profile_id {
      return Err(WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!(
          "Worker profile ({}) does not match request profile ({})",
          worker.profile_id, profile_id
        ),
      ));
    }

    if worker.current_lease_id.as_deref() != Some(lease_id) {
      return Err(WorkerError::new(
        WorkerErrorCode::CorrelationMismatch,
        format!(
          "Worker active lease mismatch: worker current_lease={:?}, requested lease={}",
          worker.current_lease_id, lease_id
        ),
      ));
    }

    Ok(lease.clone())
  }

  /// Safe release: Released lease moves worker to RECONCILING (NOT immediately READY).
  pub async fn release(&self, lease_id: &str) -> ReleaseLeaseResponse {
    let mut state = self.state.lock().await;

    let worker_id_opt = if let Some(lease) = state.leases.get_mut(lease_id) {
      lease.status = LeaseStatus::Released;
      Some(lease.worker_id.clone())
    } else {
      None
    };

    if let Some(ref worker_id) = worker_id_opt {
      if let Some(w) = state.workers.get_mut(worker_id) {
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
    ListWorkersResponse {
      workers: list,
      total,
    }
  }

  pub async fn list_leases(&self) -> ListLeasesResponse {
    let state = self.state.lock().await;
    let list: Vec<WorkerLease> = state.leases.values().cloned().collect();
    let total = list.len();
    ListLeasesResponse {
      leases: list,
      total,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  async fn seed_test_worker(
    registry: &WorkerRegistry,
    worker_id: &str,
    profile_id: &str,
    pool_id: Option<&str>,
  ) {
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
    let mut state = registry.state.lock().await;
    state.is_ready = true;
    state.workers.insert(worker_id.to_string(), worker);
  }

  #[test]
  fn test_01_production_new_does_not_seed_fake_workers() {
    let registry = WorkerRegistry::new();
    let state = registry.state.blocking_lock();
    assert_eq!(
      state.workers.len(),
      0,
      "Production WorkerRegistry::new() must start empty"
    );
    assert!(
      !state.is_ready,
      "Production WorkerRegistry starts in INITIALIZING state"
    );
  }

  #[tokio::test]
  async fn test_02_real_runtime_registration_then_health_handshake() {
    let registry = WorkerRegistry::new();
    registry.mark_ready().await;
    let worker = BrowserWorker {
      worker_id: "browser-profile:PROFILE_A".to_string(),
      profile_id: "PROFILE_A".to_string(),
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
    registry
      .handle_health_handshake(
        "browser-profile:PROFILE_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_A".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("browser-profile:PROFILE_A").unwrap();
    assert_eq!(w.state, WorkerState::Ready);
    assert_eq!(w.grok_logged_in, Some(true));
  }

  #[tokio::test]
  async fn test_03_health_for_unknown_worker_rejected() {
    let registry = WorkerRegistry::new();
    registry.mark_ready().await;
    let err = registry
      .handle_health_handshake(
        "UNKNOWN_WORKER",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_X".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::NoAvailableWorker);
  }

  #[tokio::test]
  async fn test_04_profile_mismatch_fails() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "browser-profile:PROFILE_A", "PROFILE_A", None).await;

    let err = registry
      .handle_health_handshake(
        "browser-profile:PROFILE_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_B".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::InvalidProfile);
    let state = registry.state.lock().await;
    let w = state.workers.get("browser-profile:PROFILE_A").unwrap();
    assert_eq!(w.state, WorkerState::Error);
  }

  #[tokio::test]
  async fn test_05_protocol_mismatch_fails() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "browser-profile:PROFILE_A", "PROFILE_A", None).await;

    let err = registry
      .handle_health_handshake(
        "browser-profile:PROFILE_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_A".to_string(),
          protocol_version: 2, // incompatible
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::ProtocolMismatch);
  }

  #[tokio::test]
  async fn test_06_release_moves_worker_to_reconciling() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    let res = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_001".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    // Release lease
    registry.release(&res.lease_id).await;

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(
      w.state,
      WorkerState::Reconciling,
      "Released worker must be RECONCILING, not immediately READY"
    );
  }

  #[tokio::test]
  async fn test_07_health_idle_after_release_transitions_to_ready() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    let res = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_001".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    registry.release(&res.lease_id).await;

    // Health probe returns IDLE
    registry
      .handle_health_handshake(
        "WORKER_01",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_GROK_01".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(w.state, WorkerState::Ready);
  }

  #[tokio::test]
  async fn test_08_health_busy_after_release_remains_non_schedulable() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    let res = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_001".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    registry.release(&res.lease_id).await;

    // Health probe returns BUSY
    registry
      .handle_health_handshake(
        "WORKER_01",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_GROK_01".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "BUSY".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    {
      let state = registry.state.lock().await;
      let w = state.workers.get("WORKER_01").unwrap();
      assert_eq!(w.state, WorkerState::Busy);
    }

    // Cannot acquire busy worker
    let err = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_002".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_09_logout_after_release_sets_login_required() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    let res = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_001".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    registry.release(&res.lease_id).await;

    // Health probe returns logged_in = false
    registry
      .handle_health_handshake(
        "WORKER_01",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_GROK_01".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: false,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(w.state, WorkerState::LoginRequired);
  }

  #[tokio::test]
  async fn test_10_startup_active_lease_recovery_prevents_double_lease() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_A", "PROFILE_A", None).await;

    let active_lease = WorkerLease {
      lease_id: "LEASE_PERSISTED_A".to_string(),
      worker_id: "WORKER_A".to_string(),
      profile_id: "PROFILE_A".to_string(),
      job_id: "JOB_A".to_string(),
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
    let err = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_B".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_11_startup_recovered_busy_extension_retains_ownership() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_A", "PROFILE_A", None).await;

    let active_lease = WorkerLease {
      lease_id: "LEASE_A".to_string(),
      worker_id: "WORKER_A".to_string(),
      profile_id: "PROFILE_A".to_string(),
      job_id: "JOB_A".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      status: LeaseStatus::Active,
      acquired_at: Utc::now().to_rfc3339(),
      expires_at: (Utc::now() + Duration::seconds(120)).to_rfc3339(),
      last_heartbeat_at: Utc::now().to_rfc3339(),
    };

    registry.recover_leases(vec![active_lease]).await;

    // Probe says still busy
    registry
      .handle_health_handshake(
        "WORKER_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_A".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "BUSY".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_A").unwrap();
    assert_eq!(w.state, WorkerState::Busy);
  }

  #[tokio::test]
  async fn test_12_startup_recovered_idle_extension_terminalizes_stale_lease_safely() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_A", "PROFILE_A", None).await;

    let active_lease = WorkerLease {
      lease_id: "LEASE_A".to_string(),
      worker_id: "WORKER_A".to_string(),
      profile_id: "PROFILE_A".to_string(),
      job_id: "JOB_A".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      status: LeaseStatus::Active,
      acquired_at: Utc::now().to_rfc3339(),
      expires_at: (Utc::now() + Duration::seconds(120)).to_rfc3339(),
      last_heartbeat_at: Utc::now().to_rfc3339(),
    };

    registry.recover_leases(vec![active_lease]).await;

    // Release/terminalize stale lease on daemon restart
    registry.release("LEASE_A").await;

    // Health probe confirms IDLE
    registry
      .handle_health_handshake(
        "WORKER_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_A".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_A").unwrap();
    assert_eq!(w.state, WorkerState::Ready);
  }

  #[tokio::test]
  async fn test_13_recovered_lease_wrong_profile_fails() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_A", "PROFILE_A", None).await;

    let err = registry
      .handle_health_handshake(
        "WORKER_A",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_WRONG".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::InvalidProfile);
  }

  #[tokio::test]
  async fn test_14_100_acquire_release_reconcile_sequential_cycles_no_deadlock() {
    let registry = Arc::new(WorkerRegistry::new());
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    for i in 0..100 {
      let res = registry
        .acquire(AcquireWorkerRequest {
          job_id: format!("JOB_{i}"),
          step_id: "GENERATING_IMAGE".to_string(),
          attempt_id: "ATTEMPT_001".to_string(),
          capability: "grok.image.edit".to_string(),
          pool_id: None,
          profile_id: None,
          ttl_seconds: Some(120),
        })
        .await
        .unwrap();

      registry.release(&res.lease_id).await;
      registry
        .reconcile_worker("WORKER_01", true, true, Some(true))
        .await
        .unwrap();
    }

    let state = registry.state.lock().await;
    let w = state.workers.get("WORKER_01").unwrap();
    assert_eq!(w.state, WorkerState::Ready);
  }

  #[tokio::test]
  async fn test_15_double_acquire_without_release_returns_worker_busy() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None).await;

    let _res1 = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_A".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    let err = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_B".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_16_sequential_reuse_different_jobs() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_A", None).await;

    // Job 1 acquires
    let res1 = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_001".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();
    assert_eq!(res1.profile_id, "PROFILE_A");

    // Job 1 releases
    registry.release(&res1.lease_id).await;

    // Health reports IDLE
    registry
      .handle_health_handshake(
        "WORKER_01",
        WorkerHealthHandshakeRequest {
          profile_id: "PROFILE_A".to_string(),
          protocol_version: 1,
          extension_version: "1.1.49".to_string(),
          worker_state: "IDLE".to_string(),
          logged_in: true,
          capabilities: vec!["grok.image.edit".to_string()],
        },
      )
      .await
      .unwrap();

    // Job 2 acquires same profile with new lease
    let res2 = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_002".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();
    assert_eq!(res2.profile_id, "PROFILE_A");
    assert_ne!(res1.lease_id, res2.lease_id);
  }

  // TEST GROUP D — Startup readiness barrier
  #[tokio::test]
  async fn test_d1_acquire_during_initializing_returns_503_registry_initializing() {
    let registry = WorkerRegistry::new();
    // Initially not marked ready
    assert!(!registry.is_ready().await);

    let err = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_INIT_01".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;

    assert!(err.is_err());
    let e = err.unwrap_err();
    assert_eq!(e.code, WorkerErrorCode::WorkerRegistryInitializing);
    assert_eq!(e.status_code(), 503);
  }

  #[tokio::test]
  async fn test_d2_load_persisted_active_lease_denies_acquire_same_profile() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_ACTIVE", "PROFILE_PERSISTED", None).await;

    let active_lease = WorkerLease {
      lease_id: "LEASE_PERSISTED_001".to_string(),
      worker_id: "WORKER_ACTIVE".to_string(),
      profile_id: "PROFILE_PERSISTED".to_string(),
      job_id: "JOB_PERSISTED".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      status: LeaseStatus::Active,
      acquired_at: Utc::now().to_rfc3339(),
      expires_at: (Utc::now() + Duration::seconds(120)).to_rfc3339(),
      last_heartbeat_at: Utc::now().to_rfc3339(),
    };

    registry.recover_leases(vec![active_lease]).await;
    registry.mark_ready().await;

    let err = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_NEW_02".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;

    assert!(err.is_err());
    assert_eq!(err.unwrap_err().code, WorkerErrorCode::WorkerBusy);
  }

  #[tokio::test]
  async fn test_d3_registry_ready_allows_eligible_acquire() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_READY", None).await;
    registry.mark_ready().await;

    let res = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_READY_01".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await;

    assert!(res.is_ok());
    assert_eq!(res.unwrap().profile_id, "PROFILE_READY");
  }

  #[tokio::test]
  async fn test_d4_corrupt_storage_fails_safely_without_silent_wipe() {
    let temp_dir = std::env::temp_dir().join(format!("donut_test_{}", Uuid::new_v4()));
    let _ = fs::create_dir_all(&temp_dir);
    let corrupt_file = temp_dir.join("corrupt_leases.json");
    let _ = fs::write(&corrupt_file, b"NOT_VALID_JSON{[[");

    let registry = WorkerRegistry::with_custom_storage(Some(corrupt_file.clone()));
    registry.load_from_storage().await;

    // File should remain on disk and not silently wiped
    assert!(corrupt_file.exists());
    let _ = fs::remove_dir_all(&temp_dir);
  }

  #[tokio::test]
  async fn test_validate_active_lease_exact_matches_success() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_A", None).await;

    let acq = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_100".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    let res = registry
      .validate_active_lease(
        &acq.lease_id,
        "JOB_100",
        "GENERATING_IMAGE",
        "ATTEMPT_001",
        "PROFILE_A",
      )
      .await;
    assert!(res.is_ok());
  }

  #[tokio::test]
  async fn test_validate_active_lease_mismatches_fail() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_A", None).await;

    let acq = registry
      .acquire(AcquireWorkerRequest {
        job_id: "JOB_100".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        profile_id: None,
        ttl_seconds: Some(120),
      })
      .await
      .unwrap();

    // Wrong job -> fail
    let err_job = registry
      .validate_active_lease(
        &acq.lease_id,
        "JOB_WRONG",
        "GENERATING_IMAGE",
        "ATTEMPT_001",
        "PROFILE_A",
      )
      .await;
    assert!(err_job.is_err());
    assert_eq!(
      err_job.unwrap_err().code,
      WorkerErrorCode::CorrelationMismatch
    );

    // Wrong profile -> fail
    let err_prof = registry
      .validate_active_lease(
        &acq.lease_id,
        "JOB_100",
        "GENERATING_IMAGE",
        "ATTEMPT_001",
        "PROFILE_WRONG",
      )
      .await;
    assert!(err_prof.is_err());
    assert_eq!(err_prof.unwrap_err().code, WorkerErrorCode::InvalidProfile);

    // Released lease -> fail
    registry.release(&acq.lease_id).await;
    let err_rel = registry
      .validate_active_lease(
        &acq.lease_id,
        "JOB_100",
        "GENERATING_IMAGE",
        "ATTEMPT_001",
        "PROFILE_A",
      )
      .await;
    assert!(err_rel.is_err());
    assert_eq!(err_rel.unwrap_err().code, WorkerErrorCode::LeaseNotActive);
  }
}
