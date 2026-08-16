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

#[derive(Clone)]
pub struct WorkerRegistry {
  workers: Arc<Mutex<HashMap<String, BrowserWorker>>>,
  leases: Arc<Mutex<HashMap<String, WorkerLease>>>,
}

impl Default for WorkerRegistry {
  fn default() -> Self {
    Self::new()
  }
}

impl WorkerRegistry {
  pub fn new() -> Self {
    let mut initial_workers = HashMap::new();

    // Seed default generation workers for local profile slots
    for i in 1..=5 {
      let worker_id = format!("WORKER_{:02}", i);
      let profile_id = format!("PROFILE_GROK_{:02}", i);
      initial_workers.insert(
        worker_id.clone(),
        BrowserWorker {
          worker_id,
          profile_id,
          state: WorkerState::Ready,
          capabilities: vec![
            "grok.image.edit".to_string(),
            "grok.image.expand_9_16".to_string(),
            "grok.video.generate".to_string(),
            "grok.media.resolve".to_string(),
          ],
          extension_ready: true,
          extension_version: Some("1.1.49".to_string()),
          protocol_version: Some(1),
          grok_logged_in: Some(true),
          current_lease_id: None,
          current_job_id: None,
          last_heartbeat_at: Some(Utc::now().to_rfc3339()),
          last_error: None,
        },
      );
    }

    Self {
      workers: Arc::new(Mutex::new(initial_workers)),
      leases: Arc::new(Mutex::new(HashMap::new())),
    }
  }

  /// Atomically acquires an exclusive worker lease for a specific step attempt.
  pub async fn acquire(&self, req: AcquireWorkerRequest) -> Result<AcquireWorkerResponse, (u16, String)> {
    let mut workers = self.workers.lock().await;
    let mut leases = self.leases.lock().await;

    // First reap expired leases to free up any stale slots
    let now = Utc::now();
    for lease in leases.values_mut() {
      if lease.status == LeaseStatus::Active {
        if let Ok(exp) = DateTime::parse_from_rfc3339(&lease.expires_at) {
          if now > exp.with_timezone(&Utc) {
            lease.status = LeaseStatus::Expired;
            if let Some(w) = workers.get_mut(&lease.worker_id) {
              if w.current_lease_id.as_deref() == Some(&lease.lease_id) {
                w.current_lease_id = None;
                w.current_job_id = None;
                w.state = WorkerState::Ready;
              }
            }
          }
        }
      }
    }

    // Filter available worker matching capability and ready status
    let eligible_worker_id = workers
      .values()
      .find(|w| {
        w.state == WorkerState::Ready
          && w.current_lease_id.is_none()
          && w.extension_ready
          && w.grok_logged_in.unwrap_or(false)
          && w.capabilities.iter().any(|c| c == &req.capability)
      })
      .map(|w| w.worker_id.clone());

    let worker_id = match eligible_worker_id {
      Some(id) => id,
      None => {
        // Check if workers exist but are all busy
        let any_busy = workers
          .values()
          .any(|w| w.capabilities.iter().any(|c| c == &req.capability));
        if any_busy {
          return Err((409, "WORKER_BUSY: All eligible workers are currently leased".to_string()));
        } else {
          return Err((409, "NO_AVAILABLE_WORKER: No worker found with requested capability".to_string()));
        }
      }
    };

    let worker = workers.get_mut(&worker_id).unwrap();
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

    leases.insert(lease_id.clone(), lease);

    Ok(AcquireWorkerResponse {
      lease_id,
      worker_id: worker.worker_id.clone(),
      profile_id: worker.profile_id.clone(),
      expires_at,
    })
  }

  /// Renews lease expiration timestamp via heartbeat.
  pub async fn heartbeat(&self, lease_id: &str, req: HeartbeatLeaseRequest) -> Result<HeartbeatLeaseResponse, (u16, String)> {
    let mut leases = self.leases.lock().await;
    let mut workers = self.workers.lock().await;

    let lease = leases
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

    if let Some(w) = workers.get_mut(&lease.worker_id) {
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
    let mut leases = self.leases.lock().await;
    let mut workers = self.workers.lock().await;

    if let Some(lease) = leases.get_mut(lease_id) {
      lease.status = LeaseStatus::Released;
      if let Some(w) = workers.get_mut(&lease.worker_id) {
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

  pub async fn list_workers(&self) -> ListWorkersResponse {
    let workers = self.workers.lock().await;
    let list: Vec<BrowserWorker> = workers.values().cloned().collect();
    let total = list.len();
    ListWorkersResponse { workers: list, total }
  }

  pub async fn list_leases(&self) -> ListLeasesResponse {
    let leases = self.leases.lock().await;
    let list: Vec<WorkerLease> = leases.values().cloned().collect();
    let total = list.len();
    ListLeasesResponse { leases: list, total }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn test_acquire_and_idempotent_release() {
    let registry = WorkerRegistry::new();

    let req = AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(120),
    };

    let res = registry.acquire(req).await.expect("Must acquire available worker");
    assert!(!res.lease_id.is_empty());
    assert!(!res.profile_id.is_empty());

    // Release once
    let rel1 = registry.release(&res.lease_id).await;
    assert_eq!(rel1.status, LeaseStatus::Released);

    // Release second time (idempotent)
    let rel2 = registry.release(&res.lease_id).await;
    assert_eq!(rel2.status, LeaseStatus::Released);
  }

  #[tokio::test]
  async fn test_heartbeat_correlation() {
    let registry = WorkerRegistry::new();

    let req = AcquireWorkerRequest {
      job_id: "JOB_001".to_string(),
      step_id: "GENERATING_IMAGE".to_string(),
      attempt_id: "ATTEMPT_001".to_string(),
      capability: "grok.image.edit".to_string(),
      pool_id: None,
      ttl_seconds: Some(60),
    };

    let res = registry.acquire(req).await.expect("Must acquire available worker");

    // Valid heartbeat
    let hb = registry
      .heartbeat(
        &res.lease_id,
        HeartbeatLeaseRequest {
          job_id: "JOB_001".to_string(),
          attempt_id: "ATTEMPT_001".to_string(),
          ttl_seconds: Some(120),
        },
      )
      .await
      .expect("Valid heartbeat must succeed");
    assert_eq!(hb.status, LeaseStatus::Active);

    // Mismatched attempt_id heartbeat
    let err = registry
      .heartbeat(
        &res.lease_id,
        HeartbeatLeaseRequest {
          job_id: "JOB_001".to_string(),
          attempt_id: "ATTEMPT_002".to_string(),
          ttl_seconds: Some(120),
        },
      )
      .await;
    assert!(err.is_err());
  }
}

