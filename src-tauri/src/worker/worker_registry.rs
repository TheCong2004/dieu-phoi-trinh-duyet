use super::worker_types::*;
use chrono::{DateTime, Duration, Utc};
use lazy_static::lazy_static;
use std::collections::HashMap;
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
    Self {
      state: Arc::new(Mutex::new(RegistryState::default())),
    }
  }

  /// Register or update a browser worker based on real runtime / extension health handshake.
  pub async fn register_or_update_worker(&self, worker: BrowserWorker) -> Result<(), (u16, String)> {
    let mut state = self.state.lock().await;

    // Validate protocol version if extension is connected
    if let Some(proto) = worker.protocol_version {
      if proto != 1 {
        return Err((400, format!("PROTOCOL_MISMATCH: Incompatible extension protocol version {proto}")));
      }
    }

    state.workers.insert(worker.worker_id.clone(), worker);
    Ok(())
  }

  /// Atomically acquires an exclusive worker lease for a specific step attempt.
  pub async fn acquire(&self, req: AcquireWorkerRequest) -> Result<AcquireWorkerResponse, (u16, String)> {
    let mut state = self.state.lock().await;
    let now = Utc::now();

    // 1. First reap expired leases safely (marks worker RECONCILING, never blindly READY)
    for lease in state.leases.values_mut() {
      if lease.status == LeaseStatus::Active {
        if let Ok(exp) = DateTime::parse_from_rfc3339(&lease.expires_at) {
          if now > exp.with_timezone(&Utc) {
            lease.status = LeaseStatus::Expired;
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
        return Err((404, format!("POOL_NOT_FOUND: Worker pool '{pool}' does not exist")));
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
          return Err((409, "WORKER_BUSY: All eligible workers are currently leased or reconciling".to_string()));
        } else {
          return Err((409, "NO_AVAILABLE_WORKER: No worker found with requested capability".to_string()));
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

    Ok(AcquireWorkerResponse {
      lease_id,
      worker_id: worker.worker_id.clone(),
      profile_id: worker.profile_id.clone(),
      expires_at,
    })
  }

  /// Renews lease expiration timestamp via heartbeat with strict correlation check.
  pub async fn heartbeat(&self, lease_id: &str, req: HeartbeatLeaseRequest) -> Result<HeartbeatLeaseResponse, (u16, String)> {
    let mut state = self.state.lock().await;

    let lease = state
      .leases
      .get_mut(lease_id)
      .ok_or((404, "Lease not found".to_string()))?;

    if lease.status != LeaseStatus::Active {
      return Err((409, format!("Lease is not active: {:?}", lease.status)));
    }

    if lease.job_id != req.job_id || lease.attempt_id != req.attempt_id {
      return Err((400, "Lease correlation mismatch for heartbeat".to_string()));
    }

    let now = Utc::now();
    let ttl_secs = req.ttl_seconds.unwrap_or(60);
    let expires_at = (now + Duration::seconds(ttl_secs as i64)).to_rfc3339();

    lease.expires_at = expires_at.clone();
    lease.last_heartbeat_at = now.to_rfc3339();

    if let Some(w) = state.workers.get_mut(&lease.worker_id) {
      w.last_heartbeat_at = Some(now.to_rfc3339());
    }

    Ok(HeartbeatLeaseResponse {
      lease_id: lease_id.to_string(),
      status: LeaseStatus::Active,
      expires_at,
    })
  }

  /// Idempotently releases a worker lease.
  pub async fn release(&self, lease_id: &str) -> ReleaseLeaseResponse {
    let mut state = self.state.lock().await;

    if let Some(lease) = state.leases.get_mut(lease_id) {
      lease.status = LeaseStatus::Released;
      if let Some(w) = state.workers.get_mut(&lease.worker_id) {
        if w.current_lease_id.as_deref() == Some(lease_id) {
          w.current_lease_id = None;
          w.current_job_id = None;
          w.state = WorkerState::Ready;
        }
      }
    }

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
        } else {
          // Recover active lease ownership
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
  fn test_18_production_new_does_not_seed_fake_workers() {
    let registry = WorkerRegistry::new();
    let state = registry.state.blocking_lock();
    assert_eq!(state.workers.len(), 0, "Production WorkerRegistry::new() must start empty");
  }

  #[tokio::test]
  async fn test_01_acquire_active_lease() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let req = AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    };

    let res = registry.acquire(req).await.expect("Must acquire available worker");
    assert_eq!(res.worker_id, "WORKER_01");
    assert_eq!(res.profile_id, "PROFILE_GROK_01");
  }

  #[tokio::test]
  async fn test_02_second_acquire_same_worker_rejected() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let req1 = AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    };
    registry.acquire(req1).await.unwrap();

    let req2 = AcquireWorkerRequest {
      job_id: "JOB_002".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    };
    let err = registry.acquire(req2).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().0, 409);
  }

  #[tokio::test]
  async fn test_03_concurrent_acquire_no_double_lease() {
    let registry = Arc::new(WorkerRegistry::new());
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let r1 = registry.clone();
    let r2 = registry.clone();

    let t1 = tokio::spawn(async move {
      r1.acquire(AcquireWorkerRequest {
        job_id: "JOB_A".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        ttl_seconds: Some(120),
      }).await
    });

    let t2 = tokio::spawn(async move {
      r2.acquire(AcquireWorkerRequest {
        job_id: "JOB_B".to_string(),
        step_id: "GENERATING_IMAGE".to_string(),
        attempt_id: "ATTEMPT_001".to_string(),
        capability: "grok.image.edit".to_string(),
        pool_id: None,
        ttl_seconds: Some(120),
      }).await
    });

    let (res1, res2) = tokio::join!(t1, t2);
    let r1 = res1.unwrap();
    let r2 = res2.unwrap();

    // Exactly one must succeed and one must fail
    assert!(r1.is_ok() ^ r2.is_ok(), "Only one concurrent request can acquire the single worker");
  }

  #[tokio::test]
  async fn test_04_100_acquire_release_cycles_no_deadlock() {
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
    }
  }

  #[tokio::test]
  async fn test_05_06_heartbeat_correlation() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(60),
    }).await.unwrap();

    // Valid heartbeat
    let hb = registry.heartbeat(&res.lease_id, HeartbeatLeaseRequest {
      job_id: "JOB_001".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      ttl_seconds: Some(120),
    }).await;
    assert!(hb.is_ok());

    // Mismatched attempt
    let hb_bad = registry.heartbeat(&res.lease_id, HeartbeatLeaseRequest {
      job_id: "JOB_001".to_string(),
      attempt_id: "ATTEMPT_002".to_string(),
      ttl_seconds: Some(120),
    }).await;
    assert!(hb_bad.is_err());
  }

  #[tokio::test]
  async fn test_07_release_twice_idempotent() {
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

    let r1 = registry.release(&res.lease_id).await;
    assert_eq!(r1.status, LeaseStatus::Released);

    let r2 = registry.release(&res.lease_id).await;
    assert_eq!(r2.status, LeaseStatus::Released);
  }

  #[tokio::test]
  async fn test_08_expired_lease_moves_worker_to_reconciling_not_ready() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", None);

    // Acquire with negative/past TTL
    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(0), // expires immediately
    }).await.unwrap();

    // Trigger reaper via next acquire
    let _ = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_002".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(60),
    }).await;

    let state = registry.state.lock().await;
    let worker = state.workers.get("WORKER_01").unwrap();
    assert_eq!(worker.state, WorkerState::Reconciling, "Expired worker must move to Reconciling, not Ready");
  }

  #[tokio::test]
  async fn test_16_17_pool_filtering_and_invalid_pool() {
    let registry = WorkerRegistry::new();
    seed_test_worker(&registry, "WORKER_01", "PROFILE_GROK_01", Some("GROK_POOL_A"));
    seed_test_worker(&registry, "WORKER_02", "PROFILE_GROK_02", Some("GROK_POOL_B"));

    // Valid pool
    let res = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: Some("GROK_POOL_A".to_string()),
      ttl_seconds: Some(120),
    }).await.unwrap();
    assert_eq!(res.worker_id, "WORKER_01");

    // Invalid pool
    let err = registry.acquire(AcquireWorkerRequest {
      job_id: "JOB_002".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: Some("NON_EXISTENT_POOL".to_string()),
      ttl_seconds: Some(120),
    }).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().0, 404);
  }
}
