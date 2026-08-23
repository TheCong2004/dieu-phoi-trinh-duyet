use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// Canonical Floword production capability names. These are the only names
// persisted in the worker registry and used by lease/dispatch validation.
pub const GROK_IMAGE_EDIT: &str = "grok.image.edit";
pub const GROK_IMAGE_EXPAND_9_16: &str = "grok.image.expand_9_16";
pub const GROK_VIDEO_GENERATE: &str = "grok.video.generate";

/// Normalize capability names received from older extension builds. Unknown
/// capabilities are preserved so the runtime never grants capabilities that
/// were not actually reported by the extension handshake.
pub fn normalize_capability(raw: &str) -> String {
  match raw.trim() {
    "grok_image_edit" => GROK_IMAGE_EDIT.to_string(),
    "grok_expand_9_16" | "grok.expand.9_16" => GROK_IMAGE_EXPAND_9_16.to_string(),
    "grok_video_generate" => GROK_VIDEO_GENERATE.to_string(),
    value => value.to_string(),
  }
}

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerProvider {
  Playwright,
  Wayfern,
}

impl Default for WorkerProvider {
  fn default() -> Self {
    Self::Wayfern
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LeaseStatus {
  Active,
  Released,
  Expired,
  Revoked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ProductionSite {
  Grok,
  Facebook,
  TikTok,
  YouTubeStudio,
}

impl ProductionSite {
  pub fn display_name(&self) -> &'static str {
    match self {
      ProductionSite::Grok => "grok.com",
      ProductionSite::Facebook => "facebook.com",
      ProductionSite::TikTok => "tiktok.com",
      ProductionSite::YouTubeStudio => "studio.youtube.com",
    }
  }

  /// Safe host matching using exact host or valid subdomains (never loose substring matching)
  /// Note: YouTubeStudio strictly matches ONLY "studio.youtube.com" (Section 14).
  pub fn matches_host(&self, host: &str) -> bool {
    let lower_host = host.to_ascii_lowercase();
    match self {
      ProductionSite::Grok => lower_host == "grok.com" || lower_host.ends_with(".grok.com"),
      ProductionSite::Facebook => {
        lower_host == "facebook.com" || lower_host.ends_with(".facebook.com")
      }
      ProductionSite::TikTok => lower_host == "tiktok.com" || lower_host.ends_with(".tiktok.com"),
      ProductionSite::YouTubeStudio => lower_host == "studio.youtube.com",
    }
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ProductionSitePolicy {
  Site(ProductionSite),
  ActiveTask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SiteSessionState {
  Ready,
  AuthRequired,
  Unknown,
  Unsupported,
  Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerSiteSession {
  pub site: ProductionSite,
  pub state: SiteSessionState,
  pub checked_at: Option<String>,
  pub current_host: Option<String>,
  pub account_identifier: Option<String>,
  pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductionMethodDescriptor {
  pub method: &'static str,
  pub required_capability: &'static str,
  pub site_policy: ProductionSitePolicy,
  pub requires_auth: bool,
  pub implemented: bool,
}

impl ProductionMethodDescriptor {
  pub fn lookup(method: &str) -> Option<ProductionMethodDescriptor> {
    match method {
      // Grok methods
      "grok.health" => Some(ProductionMethodDescriptor {
        method: "grok.health",
        required_capability: "grok.health",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: false,
        implemented: true,
      }),
      "grok.image.edit" => Some(ProductionMethodDescriptor {
        method: "grok.image.edit",
        required_capability: GROK_IMAGE_EDIT,
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: true,
        implemented: true,
      }),
      "grok.image.expand_9_16" => Some(ProductionMethodDescriptor {
        method: "grok.image.expand_9_16",
        required_capability: GROK_IMAGE_EXPAND_9_16,
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: true,
        implemented: true,
      }),
      "grok.video.generate" => Some(ProductionMethodDescriptor {
        method: "grok.video.generate",
        required_capability: GROK_VIDEO_GENERATE,
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: true,
        implemented: true,
      }),
      "grok.generation.status" => Some(ProductionMethodDescriptor {
        method: "grok.generation.status",
        required_capability: "grok.generation.status",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: true,
        implemented: false,
      }),
      "grok.media.resolve" => Some(ProductionMethodDescriptor {
        method: "grok.media.resolve",
        required_capability: "grok.media.resolve",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: false,
        implemented: false,
      }),
      "grok.media.download" => Some(ProductionMethodDescriptor {
        method: "grok.media.download",
        required_capability: "grok.media.download",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Grok),
        requires_auth: false,
        implemented: false,
      }),
      "production.task.cancel" => Some(ProductionMethodDescriptor {
        method: "production.task.cancel",
        required_capability: "production.task.cancel",
        site_policy: ProductionSitePolicy::ActiveTask,
        requires_auth: false,
        implemented: true,
      }),

      // Social Health methods
      "social.facebook.health" => Some(ProductionMethodDescriptor {
        method: "social.facebook.health",
        required_capability: "social.facebook.health",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Facebook),
        requires_auth: false,
        implemented: true,
      }),
      "social.tiktok.health" => Some(ProductionMethodDescriptor {
        method: "social.tiktok.health",
        required_capability: "social.tiktok.health",
        site_policy: ProductionSitePolicy::Site(ProductionSite::TikTok),
        requires_auth: false,
        implemented: true,
      }),
      "social.youtube.health" => Some(ProductionMethodDescriptor {
        method: "social.youtube.health",
        required_capability: "social.youtube.health",
        site_policy: ProductionSitePolicy::Site(ProductionSite::YouTubeStudio),
        requires_auth: false,
        implemented: true,
      }),

      // Known Social Publish methods (Known contract, but unimplemented in this foundation phase)
      "social.facebook.reels.publish" => Some(ProductionMethodDescriptor {
        method: "social.facebook.reels.publish",
        required_capability: "social.facebook.publish",
        site_policy: ProductionSitePolicy::Site(ProductionSite::Facebook),
        requires_auth: true,
        implemented: false,
      }),
      "social.tiktok.video.publish" => Some(ProductionMethodDescriptor {
        method: "social.tiktok.video.publish",
        required_capability: "social.tiktok.publish",
        site_policy: ProductionSitePolicy::Site(ProductionSite::TikTok),
        requires_auth: true,
        implemented: false,
      }),
      "social.youtube.shorts.publish" => Some(ProductionMethodDescriptor {
        method: "social.youtube.shorts.publish",
        required_capability: "social.youtube.publish",
        site_policy: ProductionSitePolicy::Site(ProductionSite::YouTubeStudio),
        requires_auth: true,
        implemented: false,
      }),

      _ => None,
    }
  }
}

/// Centralized capability policy mapping capability strings to site and authentication requirements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityPolicy {
  pub capability: &'static str,
  pub site: ProductionSite,
  pub requires_auth: bool,
}

impl CapabilityPolicy {
  pub fn lookup(capability: &str) -> Option<CapabilityPolicy> {
    match capability {
      "grok.health" => Some(CapabilityPolicy {
        capability: "grok.health",
        site: ProductionSite::Grok,
        requires_auth: false,
      }),
      "grok.image.edit" => Some(CapabilityPolicy {
        capability: GROK_IMAGE_EDIT,
        site: ProductionSite::Grok,
        requires_auth: true,
      }),
      "grok.image.expand_9_16" => Some(CapabilityPolicy {
        capability: GROK_IMAGE_EXPAND_9_16,
        site: ProductionSite::Grok,
        requires_auth: true,
      }),
      "grok.video.generate" => Some(CapabilityPolicy {
        capability: GROK_VIDEO_GENERATE,
        site: ProductionSite::Grok,
        requires_auth: true,
      }),
      "social.facebook.health" => Some(CapabilityPolicy {
        capability: "social.facebook.health",
        site: ProductionSite::Facebook,
        requires_auth: false,
      }),
      "social.tiktok.health" => Some(CapabilityPolicy {
        capability: "social.tiktok.health",
        site: ProductionSite::TikTok,
        requires_auth: false,
      }),
      "social.youtube.health" => Some(CapabilityPolicy {
        capability: "social.youtube.health",
        site: ProductionSite::YouTubeStudio,
        requires_auth: false,
      }),
      "social.facebook.publish" => Some(CapabilityPolicy {
        capability: "social.facebook.publish",
        site: ProductionSite::Facebook,
        requires_auth: true,
      }),
      "social.tiktok.publish" => Some(CapabilityPolicy {
        capability: "social.tiktok.publish",
        site: ProductionSite::TikTok,
        requires_auth: true,
      }),
      "social.youtube.publish" => Some(CapabilityPolicy {
        capability: "social.youtube.publish",
        site: ProductionSite::YouTubeStudio,
        requires_auth: true,
      }),
      _ => None,
    }
  }
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
  ExtensionUnavailable,
  CapabilityUnavailable,
  CapabilityMismatch,
  MethodNotSupported,
  InvalidRequest,
  TargetNotFound,
  TargetAmbiguous,
  SiteMismatch,
  SiteSessionNotReady,
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
        | WorkerErrorCode::TargetNotFound
        | WorkerErrorCode::TargetAmbiguous
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
      | WorkerErrorCode::CapabilityMismatch
      | WorkerErrorCode::WorkerReconciling
      | WorkerErrorCode::TargetAmbiguous
      | WorkerErrorCode::GrokTargetAmbiguous
      | WorkerErrorCode::GrokNotLoggedIn
      | WorkerErrorCode::SiteSessionNotReady
      | WorkerErrorCode::LeaseNotActive => 409,
      // A known capability with no eligible worker is a conflict with the
      // current worker/session state, not an unimplemented HTTP method.
      WorkerErrorCode::CapabilityUnavailable => 409,
      WorkerErrorCode::InvalidLease
      | WorkerErrorCode::CorrelationMismatch
      | WorkerErrorCode::ProtocolMismatch
      | WorkerErrorCode::MethodNotSupported
      | WorkerErrorCode::InvalidRequest
      | WorkerErrorCode::InvalidProfile
      | WorkerErrorCode::SiteMismatch => 400,
      WorkerErrorCode::InvalidHealthResponse => 502,
      WorkerErrorCode::BridgeTimeout => 504,
      WorkerErrorCode::WorkerRegistryInitializing
      | WorkerErrorCode::BridgeDisconnected
      | WorkerErrorCode::ExtensionContextNotFound
      | WorkerErrorCode::ExtensionUnavailable
      | WorkerErrorCode::TargetNotFound
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
      WorkerErrorCode::MethodNotSupported => "METHOD_NOT_SUPPORTED",
      WorkerErrorCode::InvalidRequest => "INVALID_REQUEST",
      WorkerErrorCode::InvalidProfile => "INVALID_PROFILE",
      WorkerErrorCode::WorkerReconciling => "WORKER_RECONCILING",
      WorkerErrorCode::WorkerRegistryInitializing => "WORKER_REGISTRY_INITIALIZING",
      WorkerErrorCode::BridgeDisconnected => "BRIDGE_DISCONNECTED",
      WorkerErrorCode::BridgeTimeout => "BRIDGE_TIMEOUT",
      WorkerErrorCode::ExtensionContextNotFound => "EXTENSION_CONTEXT_NOT_FOUND",
      WorkerErrorCode::ExtensionUnavailable => "EXTENSION_UNAVAILABLE",
      WorkerErrorCode::CapabilityUnavailable => "CAPABILITY_UNAVAILABLE",
      WorkerErrorCode::CapabilityMismatch => "CAPABILITY_MISMATCH",
      WorkerErrorCode::TargetNotFound => "TARGET_NOT_FOUND",
      WorkerErrorCode::TargetAmbiguous => "TARGET_AMBIGUOUS",
      WorkerErrorCode::SiteMismatch => "SITE_MISMATCH",
      WorkerErrorCode::SiteSessionNotReady => "SITE_SESSION_NOT_READY",
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
  pub grok_logged_in: Option<bool>, // Legacy Grok-specific compatibility projection
  #[serde(default)]
  pub site_sessions: std::collections::HashMap<ProductionSite, WorkerSiteSession>,
  #[serde(default)]
  pub site_capabilities: std::collections::HashMap<ProductionSite, Vec<String>>,
  pub current_lease_id: Option<String>,
  pub current_job_id: Option<String>,
  pub last_heartbeat_at: Option<String>,
  pub last_error: Option<String>,
}

impl BrowserWorker {
  pub fn provider(&self) -> WorkerProvider {
    if self.worker_id.starts_with("playwright-profile:") {
      WorkerProvider::Playwright
    } else {
      WorkerProvider::Wayfern
    }
  }
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
  pub logged_in: Option<bool>,
  #[serde(default)]
  pub capabilities: Vec<String>,
  #[serde(default)]
  pub site_sessions: Vec<WorkerSiteSession>,
  #[serde(default)]
  pub site_capabilities: std::collections::HashMap<String, Vec<String>>,
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifyPublicationRequest {
  pub publication_id: String,
  pub platform: String,
  pub profile_id: String,
  pub idempotency_key: String,
  pub target_page_id: Option<String>,
  pub target_handle: Option<String>,
  pub target_channel_id: Option<String>,
  pub external_post_id: Option<String>,
  pub target_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifyPublicationResponse {
  pub verified: bool,
  pub status: String,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub error: Option<VerifyPublicationError>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub reason: Option<String>,
  #[serde(skip_serializing_if = "Option::is_none")]
  pub details: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifyPublicationError {
  pub code: String,
  pub message: String,
  pub retryable: bool,
}

#[cfg(test)]
mod capability_contract_tests {
  use super::*;

  #[test]
  fn normalizes_legacy_grok_aliases_to_canonical_names() {
    assert_eq!(normalize_capability("grok_image_edit"), GROK_IMAGE_EDIT);
    assert_eq!(
      normalize_capability("grok_expand_9_16"),
      GROK_IMAGE_EXPAND_9_16
    );
    assert_eq!(
      normalize_capability("grok.expand.9_16"),
      GROK_IMAGE_EXPAND_9_16
    );
    assert_eq!(
      normalize_capability("grok_video_generate"),
      GROK_VIDEO_GENERATE
    );
  }

  #[test]
  fn preserves_unknown_capabilities_without_granting_defaults() {
    assert_eq!(
      normalize_capability("grok.custom.experimental"),
      "grok.custom.experimental"
    );
  }

  #[test]
  fn production_methods_require_canonical_capabilities() {
    assert_eq!(
      ProductionMethodDescriptor::lookup("grok.image.edit")
        .unwrap()
        .required_capability,
      GROK_IMAGE_EDIT
    );
    assert_eq!(
      ProductionMethodDescriptor::lookup("grok.image.expand_9_16")
        .unwrap()
        .required_capability,
      GROK_IMAGE_EXPAND_9_16
    );
    assert_eq!(
      ProductionMethodDescriptor::lookup("grok.video.generate")
        .unwrap()
        .required_capability,
      GROK_VIDEO_GENERATE
    );
  }
}
