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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkerErrorCode {
  WorkerBusy,
  NoAvailableWorker,
  PoolNotFound,
  InvalidLease,
  LeaseNotFound,
  LeaseNotActive,
  CorrelationMismatch,
  ProtocolMismatch,
  InvalidProfile,
  WorkerReconciling,
  WorkerRegistryInitializing,
  BridgeDisconnected,
  BridgeTimeout,
  ExtensionContextNotFound,
  GrokPageNotReady,
  GrokTargetAmbiguous,
  GrokNotLoggedIn,
  InvalidHealthResponse,
  Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerError {
  pub code: WorkerErrorCode,
  pub message: String,
}

impl WorkerError {
  pub fn new(code: WorkerErrorCode, message: impl Into<String>) -> Self {
    Self {
      code,
      message: message.into(),
    }
  }

  pub fn is_transient(&self) -> bool {
    matches!(
      self.code,
      WorkerErrorCode::BridgeDisconnected
        | WorkerErrorCode::BridgeTimeout
        | WorkerErrorCode::ExtensionContextNotFound
        | WorkerErrorCode::GrokPageNotReady
        | WorkerErrorCode::WorkerReconciling
        | WorkerErrorCode::GrokTargetAmbiguous
    )
  }

  pub fn status_code(&self) -> u16 {
    match self.code {
      WorkerErrorCode::PoolNotFound | WorkerErrorCode::LeaseNotFound => 404,
      WorkerErrorCode::WorkerBusy
      | WorkerErrorCode::NoAvailableWorker
      | WorkerErrorCode::WorkerReconciling
      | WorkerErrorCode::GrokTargetAmbiguous
      | WorkerErrorCode::GrokNotLoggedIn
      | WorkerErrorCode::LeaseNotActive => 409,
      WorkerErrorCode::InvalidLease
      | WorkerErrorCode::CorrelationMismatch
      | WorkerErrorCode::ProtocolMismatch
      | WorkerErrorCode::InvalidProfile => 400,
      WorkerErrorCode::InvalidHealthResponse => 502,
      WorkerErrorCode::BridgeTimeout => 504,
      WorkerErrorCode::WorkerRegistryInitializing
      | WorkerErrorCode::BridgeDisconnected
      | WorkerErrorCode::ExtensionContextNotFound
      | WorkerErrorCode::GrokPageNotReady => 503,
      WorkerErrorCode::Internal => 500,
    }
  }

  pub fn code_str(&self) -> &'static str {
    match self.code {
      WorkerErrorCode::WorkerBusy => "WORKER_BUSY",
      WorkerErrorCode::NoAvailableWorker => "NO_AVAILABLE_WORKER",
      WorkerErrorCode::PoolNotFound => "POOL_NOT_FOUND",
      WorkerErrorCode::InvalidLease => "INVALID_LEASE",
      WorkerErrorCode::LeaseNotFound => "LEASE_NOT_FOUND",
      WorkerErrorCode::LeaseNotActive => "LEASE_NOT_ACTIVE",
      WorkerErrorCode::CorrelationMismatch => "CORRELATION_MISMATCH",
      WorkerErrorCode::ProtocolMismatch => "PROTOCOL_MISMATCH",
      WorkerErrorCode::InvalidProfile => "INVALID_PROFILE",
      WorkerErrorCode::WorkerReconciling => "WORKER_RECONCILING",
      WorkerErrorCode::WorkerRegistryInitializing => "WORKER_REGISTRY_INITIALIZING",
      WorkerErrorCode::BridgeDisconnected => "BRIDGE_DISCONNECTED",
      WorkerErrorCode::BridgeTimeout => "BRIDGE_TIMEOUT",
      WorkerErrorCode::ExtensionContextNotFound => "EXTENSION_CONTEXT_NOT_FOUND",
      WorkerErrorCode::GrokPageNotReady => "GROK_PAGE_NOT_READY",
      WorkerErrorCode::GrokTargetAmbiguous => "GROK_TARGET_AMBIGUOUS",
      WorkerErrorCode::GrokNotLoggedIn => "GROK_NOT_LOGGED_IN",
      WorkerErrorCode::InvalidHealthResponse => "INVALID_HEALTH_RESPONSE",
      WorkerErrorCode::Internal => "INTERNAL_ERROR",
    }
  }
}

impl std::fmt::Display for WorkerError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}: {}", self.code_str(), self.message)
  }
}

impl std::error::Error for WorkerError {}

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
  pub profile_id: Option<String>,
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
pub struct ReconcileWorkerRequest {
  pub is_idle: bool,
  pub is_healthy: bool,
  pub grok_logged_in: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WorkerHealthHandshakeRequest {
  pub profile_id: String,
  pub protocol_version: u32,
  pub extension_version: String,
  pub worker_state: String,
  pub logged_in: bool,
  pub capabilities: Vec<String>,
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
