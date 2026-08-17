use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerState {
  Offline,
  Starting,
  Ready,
  Leased,
  Busy,
  Reconciling,
  LoginRequired,
  Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeaseStatus {
  Active,
  Released,
  Expired,
  Revoked,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BrowserWorker {
  pub worker_id: String,
  pub profile_id: String,
  pub pool_id: Option<String>,
  pub state: WorkerState,
  pub capabilities: Vec<String>,
  pub extension_ready: bool,
  pub extension_version: Option<String>,
  pub protocol_version: Option<u32>,
  pub grok_logged_in: Option<bool>,
  pub current_lease_id: Option<String>,
  pub current_job_id: Option<String>,
  pub last_heartbeat_at: Option<String>,
  pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerLease {
  pub lease_id: String,
  pub worker_id: String,
  pub profile_id: String,
  pub job_id: String,
  pub step_id: String,
  pub attempt_id: String,
  pub capability: String,
  pub status: LeaseStatus,
  pub acquired_at: String,
  pub expires_at: String,
  pub last_heartbeat_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AcquireWorkerRequest {
  pub job_id: String,
  pub step_id: String,
  pub attempt_id: String,
  pub capability: String,
  pub pool_id: Option<String>,
  pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AcquireWorkerResponse {
  pub lease_id: String,
  pub worker_id: String,
  pub profile_id: String,
  pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HeartbeatLeaseRequest {
  pub job_id: String,
  pub attempt_id: String,
  pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HeartbeatLeaseResponse {
  pub lease_id: String,
  pub status: LeaseStatus,
  pub expires_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReleaseLeaseResponse {
  pub lease_id: String,
  pub status: LeaseStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListWorkersResponse {
  pub workers: Vec<BrowserWorker>,
  pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListLeasesResponse {
  pub leases: Vec<WorkerLease>,
  pub total: usize,
}
