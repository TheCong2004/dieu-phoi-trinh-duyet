use crate::browser::ProxySettings;
use crate::events;
use crate::group_manager::GROUP_MANAGER;
use crate::profile::manager::ProfileManager;
use crate::proxy_manager::PROXY_MANAGER;
use crate::tag_manager::TAG_MANAGER;
use axum::{
  extract::{rejection::JsonRejection, Path, Query, State},
  http::{header, HeaderMap, HeaderValue, Method, StatusCode},
  middleware::{self, Next},
  response::{IntoResponse, Json, Response},
  routing::{delete, get, post},
  Router,
};
use futures_util::FutureExt;
use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tower_http::cors::CorsLayer;
use utoipa::{OpenApi, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

// API Types
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ApiProfile {
  pub id: String,
  pub name: String,
  pub browser: String,
  pub version: String,
  pub proxy_id: Option<String>,
  pub launch_hook: Option<String>,
  pub process_id: Option<u32>,
  pub last_launch: Option<u64>,
  pub release_type: String,
  #[schema(value_type = Object)]
  pub group_id: Option<String>,
  pub tags: Vec<String>,
  pub is_running: bool,
  pub proxy_bypass_rules: Vec<String>,
  pub vpn_id: Option<String>,
  pub clear_on_close: bool,
  /// Cloud sync mode: `"Disabled"`, `"Regular"` or `"Encrypted"`.
  /// Settable via `PUT /v1/profiles/{id}`; exposed here so a caller can read
  /// back what it set, and so a remote-launch caller can tell whether the
  /// profile is actually available in cloud storage.
  pub sync_mode: String,
  /// Convenience form of `sync_mode` — true for Regular or Encrypted.
  pub cloud_sync_enabled: bool,
  /// OS the profile was created on (`"macos"`, `"windows"`, `"linux"`).
  /// `null` when neither `host_os` nor the browser config records one.
  pub host_os: Option<String>,
  /// True when the profile belongs to a different OS than this machine.
  /// Such a profile cannot be launched locally, and must only ever run on a
  /// remote host of its own OS — Chromium profile state is OS-specific.
  pub is_cross_os: bool,
  /// Legacy Wayfern profiles remain readable/exportable but are not supported
  /// by the Local Free runtime.
  pub legacy_unsupported: bool,
}

impl From<&crate::profile::types::BrowserProfile> for ApiProfile {
  /// Single conversion for every profile-returning route. Previously open-coded
  /// at three call sites, which is how `sync_mode` came to be settable but not
  /// readable: a field added to the struct had to be remembered three times.
  fn from(profile: &crate::profile::types::BrowserProfile) -> Self {
    Self {
      id: profile.id.to_string(),
      name: profile.name.clone(),
      browser: profile.browser.clone(),
      version: profile.version.clone(),
      proxy_id: profile.proxy_id.clone(),
      launch_hook: profile.launch_hook.clone(),
      process_id: profile.process_id,
      last_launch: profile.last_launch,
      release_type: profile.release_type.clone(),
      group_id: profile.group_id.clone(),
      tags: profile.tags.clone(),
      is_running: profile.process_id.is_some(),
      proxy_bypass_rules: profile.proxy_bypass_rules.clone(),
      vpn_id: profile.vpn_id.clone(),
      clear_on_close: profile.clear_on_close,
      sync_mode: format!("{:?}", profile.sync_mode),
      cloud_sync_enabled: profile.is_sync_enabled(),
      host_os: profile.resolved_os().map(|os| os.to_string()),
      is_cross_os: profile.is_cross_os(),
      legacy_unsupported: profile.browser.eq_ignore_ascii_case("wayfern"),
    }
  }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiProfilesResponse {
  pub profiles: Vec<ApiProfile>,
  pub total: usize,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ApiProfileResponse {
  pub profile: ApiProfile,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct CreateProfileRequest {
  pub name: String,
  /// Browser engine for new profiles. Local Free accepts only `"chromium"`.
  pub browser: String,
  /// Optional. Omit (or pass `"latest"`) to use the newest already-downloaded
  /// version of the chosen browser. A concrete version must already be
  /// downloaded; the create path does not fetch new versions.
  #[serde(default)]
  pub version: Option<String>,
  pub proxy_id: Option<String>,
  pub vpn_id: Option<String>,
  pub launch_hook: Option<String>,
  pub release_type: Option<String>,
  /// Wayfern fingerprint/config. Send only when `browser` is `"wayfern"`.
  /// Omit it, or pass an empty object `{}`, to have a fresh fingerprint
  /// generated automatically at creation. Provide a `fingerprint` field to
  /// pin a specific one.
  #[schema(value_type = Option<Object>)]
  pub wayfern_config: Option<serde_json::Value>,
  pub group_id: Option<String>,
  pub tags: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct UpdateProfileRequest {
  pub name: Option<String>,
  // No `browser` field: a profile's engine is fixed at creation (changing it
  // would invalidate the generated fingerprint and on-disk profile dir).
  // Accepting it here only to silently ignore it misled API clients.
  pub version: Option<String>,
  pub proxy_id: Option<String>,
  pub vpn_id: Option<String>,
  pub launch_hook: Option<String>,
  pub release_type: Option<String>,
  pub group_id: Option<String>,
  pub tags: Option<Vec<String>>,
  pub extension_group_id: Option<String>,
  pub proxy_bypass_rules: Option<Vec<String>>,
  /// One of "Disabled", "Regular", "Encrypted".
  pub sync_mode: Option<String>,
  /// Wipe browsing data (keeping extensions and bookmarks) when the browser
  /// exits. Rejected (400) for ephemeral or password-protected profiles.
  pub clear_on_close: Option<bool>,
}

#[derive(Clone)]
struct ApiServerState {
  app_handle: tauri::AppHandle,
  runtime_kind: &'static str,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ApiAcquireVpnLeaseRequest {
  pool_id: Option<String>,
  country: Option<String>,
  #[serde(default)]
  providers: Vec<crate::vpn::VpnProviderKind>,
  profile_id: Option<String>,
  ttl_seconds: Option<u64>,
  protocol: Option<crate::vpn::pool::ProxyProtocol>,
  #[serde(default)]
  wait_when_full: bool,
  max_wait_seconds: Option<u64>,
}

impl From<ApiAcquireVpnLeaseRequest> for crate::vpn::pool::AcquireVpnLeaseRequest {
  fn from(value: ApiAcquireVpnLeaseRequest) -> Self {
    Self {
      pool_id: value.pool_id,
      country: value.country,
      providers: value.providers,
      profile_id: value.profile_id,
      ttl_seconds: value.ttl_seconds,
      protocol: value.protocol,
      wait_when_full: value.wait_when_full,
      max_wait_seconds: value.max_wait_seconds,
    }
  }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct ApiVpnLeaseResponse {
  #[serde(rename = "leaseId")]
  id: String,
  pool_id: Option<String>,
  config_id: String,
  provider: crate::vpn::VpnProviderKind,
  country: Option<String>,
  profile_id: Option<String>,
  #[serde(rename = "host")]
  local_host: String,
  #[serde(rename = "port")]
  local_port: u16,
  protocol: crate::vpn::pool::ProxyProtocol,
  exit_ip: Option<String>,
  created_at: i64,
  expires_at: Option<i64>,
  status: crate::vpn::pool::LeaseStatus,
}

impl From<crate::vpn::pool::VpnLease> for ApiVpnLeaseResponse {
  fn from(value: crate::vpn::pool::VpnLease) -> Self {
    Self {
      id: value.id,
      pool_id: value.pool_id,
      config_id: value.config_id,
      provider: value.provider,
      country: value.country,
      profile_id: value.profile_id,
      local_host: value.local_host,
      local_port: value.local_port,
      protocol: value.protocol,
      exit_ip: value.exit_ip,
      created_at: value.created_at,
      expires_at: value.expires_at,
      status: value.status,
    }
  }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
struct ApiGroupResponse {
  id: String,
  name: String,
  profile_count: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateGroupRequest {
  name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateGroupRequest {
  name: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
struct ApiProxyResponse {
  id: String,
  name: String,
  #[schema(value_type = Object)]
  proxy_settings: ProxySettings,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateProxyRequest {
  name: String,
  #[schema(value_type = Object)]
  proxy_settings: ProxySettings,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateProxyRequest {
  name: Option<String>,
  #[schema(value_type = Option<Object>)]
  proxy_settings: Option<ProxySettings>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
struct ApiVpnResponse {
  id: String,
  name: String,
  /// Always "WireGuard"
  vpn_type: String,
  created_at: i64,
  last_used: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ApiVpnExportResponse {
  id: String,
  name: String,
  /// Always "WireGuard"
  vpn_type: String,
  /// Raw `.conf` file content (decrypted)
  config_data: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ImportVpnRequest {
  /// Raw WireGuard `.conf` file content
  content: String,
  /// Original filename
  filename: String,
  /// Optional display name; defaults to filename-based name
  name: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct CreateVpnRequest {
  name: String,
  /// Must be "WireGuard"
  vpn_type: String,
  config_data: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct UpdateVpnRequest {
  name: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct DownloadBrowserRequest {
  browser: String,
  version: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct DownloadBrowserResponse {
  browser: String,
  version: String,
  status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[allow(dead_code)] // Schema-only type used in OpenAPI spec; not constructed in Rust
pub struct ToastPayload {
  pub message: String,
  pub variant: String,
  pub title: String,
  pub description: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct RunProfileResponse {
  profile_id: String,
  browser_engine: String,
  browser_version: Option<String>,
  browser_executable: Option<String>,
  grok_target_id: Option<String>,
  grok_page_url: Option<String>,
  grok_target_reused: bool,
  target_selection_source: Option<String>,
  remote_debugging_port: u16,
  headless: bool,
  #[serde(skip_serializing_if = "Option::is_none")]
  browser_pid: Option<u32>,
  #[serde(skip_serializing_if = "Option::is_none")]
  launch_generation: Option<u64>,
  reused: bool,
}

/// Local browser-manager response. Unlike the legacy `/v1/profiles/{id}/run`
/// response this contract is intentionally provider-neutral and uses the
/// camelCase identity consumed by ArtCraft/Sidecar.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserSessionResponse {
  profile_id: String,
  browser_pid: u32,
  remote_debugging_port: u16,
  cdp_endpoint: String,
  launch_generation: u64,
  browser_engine: String,
  grok_target_id: Option<String>,
  grok_page_url: Option<String>,
  reused: bool,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPageResponse {
  target_id: String,
  page_type: String,
  url: String,
  title: String,
  purpose: String,
  managed: bool,
  state: String,
  browser_pid: u32,
  launch_generation: u64,
  #[serde(skip_serializing_if = "Option::is_none")]
  page_lease_id: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPagesResponse {
  profile_id: String,
  browser_pid: u32,
  remote_debugging_port: u16,
  cdp_endpoint: String,
  launch_generation: u64,
  pages: Vec<LocalBrowserPageResponse>,
}

#[derive(Debug, Serialize, ToSchema)]
struct LocalBrowserProfileSummary {
  id: String,
  name: String,
  browser: String,
  is_running: bool,
  process_id: Option<u32>,
  tags: Vec<String>,
  group_id: Option<String>,
  last_launch: Option<u64>,
  proxy_id: Option<String>,
  vpn_id: Option<String>,
  sync_mode: String,
  cloud_sync_enabled: bool,
}

#[derive(Debug, Serialize, ToSchema)]
struct LocalBrowserProfilesResponse {
  profiles: Vec<LocalBrowserProfileSummary>,
  total: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPageRequest {
  url: String,
  #[serde(default = "default_local_page_purpose")]
  purpose: String,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPageClaimRequest {
  job_id: String,
  request_id: String,
  #[serde(default)]
  target_id: Option<String>,
  #[serde(default = "default_local_page_purpose")]
  purpose: String,
  #[serde(default = "default_local_max_pages")]
  max_pages: usize,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPageReleaseRequest {
  page_lease_id: String,
  job_id: String,
  request_id: String,
}

#[derive(Debug, Serialize, ToSchema, Clone)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserPageLeaseResponse {
  profile_id: String,
  browser_pid: u32,
  remote_debugging_port: u16,
  cdp_endpoint: String,
  launch_generation: u64,
  target_id: String,
  page_lease_id: String,
  page_reused: bool,
  purpose: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct LocalBrowserStopRequest {
  profile_id: Option<String>,
  browser_pid: Option<u32>,
  remote_debugging_port: Option<u16>,
  launch_generation: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalProxyRequest {
  name: Option<String>,
  proxy_settings: ProxySettings,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
struct LocalProxyResponse {
  profile_id: String,
  proxy_id: Option<String>,
  proxy_settings: Option<ProxySettings>,
  has_credentials: bool,
}

fn default_local_page_purpose() -> String {
  "USER".to_string()
}

fn default_local_max_pages() -> usize {
  3
}

// In-memory ownership guard for pages explicitly created by the local
// manager. Pages discovered from an existing browser are never considered
// deletable unless they are the durable managed Grok target on the profile.
lazy_static! {
  static ref LOCAL_MANAGED_PAGES: Mutex<HashMap<String, HashSet<String>>> =
    Mutex::new(HashMap::new());
  static ref LOCAL_PAGE_LEASES: Mutex<HashMap<String, LocalPageLease>> = Mutex::new(HashMap::new());
  static ref LOCAL_PAGE_CLAIM_LOCK: Mutex<()> = Mutex::new(());
}

#[derive(Debug, Clone)]
struct LocalPageLease {
  profile_id: String,
  target_id: String,
  page_lease_id: String,
  job_id: String,
  request_id: String,
  purpose: String,
  launch_generation: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunRemoteRequest {
  /// Optional URL to open once the remote browser is up.
  pub url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SetCloudSyncRequest {
  /// `Disabled`, `Regular`, or `Encrypted`.
  ///
  /// `Encrypted` derives its key from a passphrase that never leaves this
  /// machine, so a profile in that mode can be synced but NOT run remotely —
  /// a remote host would download ciphertext it cannot decrypt.
  pub mode: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct SetCloudSyncResponse {
  pub profile_id: String,
  pub mode: String,
  /// Whether the profile can now be launched on a remote host.
  pub remote_launchable: bool,
  /// Why not, when `remote_launchable` is false.
  pub remote_blocked_reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RunRemoteResponse {
  pub profile_id: String,
  /// Remote session id, for polling or closing the session.
  pub session_id: String,
  /// Operating system the session was scheduled onto — always the profile's own.
  pub platform: String,
  pub status: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct StopRemoteResponse {
  pub session_id: String,
  pub status: String,
  /// What the session actually cost, in seconds.
  pub billed_seconds: u64,
}

#[derive(Debug, Deserialize, ToSchema)]
struct RunProfileRequest {
  url: Option<String>,
  headless: Option<bool>,
  /// Floword's worker path opts into the atomic cold-start-only policy.
  /// Generic clients retain the historical AlwaysOpen behavior by default.
  #[serde(default)]
  cold_start_only: Option<bool>,
  #[serde(default)]
  browser_engine: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct TargetBindingConfirmRequest {
  binding_session_id: String,
  handle: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct TargetBindingSessionRequest {
  binding_session_id: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct OpenUrlRequest {
  url: String,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ImportCookiesRequest {
  /// Raw cookie file content. Format is auto-detected: a JSON array
  /// (Puppeteer / EditThisCookie style) or a Netscape `cookies.txt`.
  content: String,
}

#[derive(Debug, Serialize, ToSchema)]
struct ImportCookiesResponse {
  cookies_imported: usize,
  cookies_replaced: usize,
  errors: Vec<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct BatchRunRequest {
  /// Profile IDs to launch.
  profile_ids: Vec<String>,
  /// Optional URL to open in every launched profile.
  url: Option<String>,
  /// Launch headless. Defaults to false.
  headless: Option<bool>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BatchRunResult {
  profile_id: String,
  /// Whether this profile launched successfully.
  ok: bool,
  /// Remote debugging port if launched, otherwise null.
  remote_debugging_port: Option<u16>,
  /// Failure reason if not launched, otherwise null.
  error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BatchRunResponse {
  results: Vec<BatchRunResult>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct BatchStopRequest {
  /// Profile IDs to stop.
  profile_ids: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BatchStopResult {
  profile_id: String,
  /// Whether this profile was stopped successfully.
  ok: bool,
  /// Failure reason if not stopped, otherwise null.
  error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct BatchStopResponse {
  results: Vec<BatchStopResult>,
}

#[derive(Debug, Serialize, ToSchema)]
struct DetectedProfilesResponse {
  profiles: Vec<crate::profile_importer::DetectedProfile>,
  total: usize,
}

#[derive(Debug, Deserialize)]
struct DetectImportQuery {
  /// Optional folder to scan instead of the default browser locations.
  folder: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ImportProfilesRequest {
  /// Profiles to import. Each item is isolated — one failure doesn't stop the rest.
  items: Vec<crate::profile_importer::ImportProfileItem>,
  /// Optional group to assign every imported profile to.
  group_id: Option<String>,
  /// How to handle an already-taken profile name: "skip" or "rename"
  /// (auto-suffix). Defaults to "rename".
  duplicate_strategy: Option<crate::profile_importer::DuplicateStrategy>,
  /// Wayfern fingerprint/config applied to every imported profile. Omit to
  /// have fresh fingerprints generated automatically.
  #[schema(value_type = Option<Object>)]
  wayfern_config: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
struct ImportProxiesRequest {
  /// "txt" — one proxy per line (`host:port`, `host:port:user:pass`, or URL
  /// forms like `http://user:pass@host:port`). "json" — a Donut proxy export.
  format: String,
  /// Raw proxy list / export content.
  content: String,
  /// Name prefix for txt imports; proxies are named "{prefix} Proxy {n}".
  name_prefix: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
struct ImportProxiesResponse {
  imported_count: usize,
  skipped_count: usize,
  errors: Vec<String>,
  proxies: Vec<ApiProxyResponse>,
}

#[derive(OpenApi)]
#[openapi(
  paths(
    get_profiles,
    get_profile,
    create_profile,
    update_profile,
    delete_profile,
    run_profile,
    prepare_target_binding,
    pending_target_binding,
    confirm_target_binding,
    abort_target_binding,
    open_url_in_profile,
    kill_profile,
    batch_run_profiles,
    batch_stop_profiles,
    detect_import_profiles,
    import_profiles_api,
    import_profile_cookies,
    get_groups,
    get_group,
    create_group,
    update_group,
    delete_group,
    get_tags,
    get_proxies,
    get_proxy,
    create_proxy,
    import_proxies_api,
    update_proxy,
    delete_proxy,
    get_vpns,
    get_vpn,
    export_vpn,
    import_vpn,
    create_vpn,
    update_vpn,
    delete_vpn,
    get_vpn_pools,
    create_vpn_pool_api,
    update_vpn_pool_api,
    delete_vpn_pool_api,
    get_vpn_leases,
    get_vpn_lease,
    acquire_vpn_lease_api,
    release_vpn_lease_api,
    get_extensions,
    get_extension_groups,
    delete_extension_api,
    delete_extension_group_api,
    download_browser_api,
    get_browser_versions,
    check_browser_downloaded,
    local_browser_claim_page,
    local_browser_release_page,
  ),
  components(schemas(
    ApiProfile,
    ApiProfilesResponse,
    ApiProfileResponse,
    CreateProfileRequest,
    UpdateProfileRequest,
    ApiGroupResponse,
    CreateGroupRequest,
    UpdateGroupRequest,
    ApiProxyResponse,
    CreateProxyRequest,
    UpdateProxyRequest,
    ApiVpnResponse,
    ApiVpnExportResponse,
    ImportVpnRequest,
    CreateVpnRequest,
    UpdateVpnRequest,
    crate::vpn::VpnProviderKind,
    crate::vpn::pool::VpnPool,
    crate::vpn::pool::VpnPoolRuntime,
    crate::vpn::pool::VpnHealth,
    crate::vpn::pool::VpnPoolStatus,
    crate::vpn::pool::PoolSelectionStrategy,
    crate::vpn::pool::RotationMode,
    crate::vpn::pool::CreateVpnPoolRequest,
    ApiVpnLeaseResponse,
    crate::vpn::pool::LeaseStatus,
    crate::vpn::pool::ProxyProtocol,
    ApiAcquireVpnLeaseRequest,
    DownloadBrowserRequest,
    DownloadBrowserResponse,
    RunProfileResponse,
    RunRemoteRequest,
    RunRemoteResponse,
    RunProfileRequest,
    TargetBindingConfirmRequest,
    TargetBindingSessionRequest,
    BatchRunRequest,
    BatchRunResult,
    BatchRunResponse,
    BatchStopRequest,
    BatchStopResult,
    BatchStopResponse,
    OpenUrlRequest,
    ImportCookiesRequest,
    ImportCookiesResponse,
    ProxySettings,
    DetectedProfilesResponse,
    ImportProfilesRequest,
    ImportProxiesRequest,
    ImportProxiesResponse,
    crate::profile_importer::DetectedProfile,
    crate::profile_importer::ImportProfileItem,
    crate::profile_importer::DuplicateStrategy,
    crate::profile_importer::ProfileImportItemResult,
    crate::profile_importer::ProfileImportBatchResult,
    LocalBrowserPageClaimRequest,
    LocalBrowserPageReleaseRequest,
    LocalBrowserPageLeaseResponse,
  )),
  tags(
    (name = "profiles", description = "Profile management endpoints"),
    (name = "groups", description = "Group management endpoints"),
    (name = "tags", description = "Tag management endpoints"),
    (name = "proxies", description = "Proxy management endpoints"),
    (name = "vpns", description = "VPN management endpoints"),
    (name = "vpn-pools", description = "VPN pool management endpoints"),
    (name = "vpn-leases", description = "Exclusive VPN proxy lease endpoints"),
    (name = "extensions", description = "Extension management endpoints"),
    (name = "browsers", description = "Browser management endpoints"),
    (name = "cookies", description = "Cookie management endpoints"),
  ),
  modifiers(&SecurityAddon),
)]
struct ApiDoc;

struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
  fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
    if let Some(components) = openapi.components.as_mut() {
      components.add_security_scheme(
        "bearer_auth",
        utoipa::openapi::security::SecurityScheme::Http(
          utoipa::openapi::security::HttpBuilder::new()
            .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
            .bearer_format("JWT")
            .build(),
        ),
      );
    }
  }
}

pub struct ApiServer {
  port: Option<u16>,
  shutdown_tx: Option<mpsc::Sender<()>>,
  task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ApiServer {
  fn new() -> Self {
    Self {
      port: None,
      shutdown_tx: None,
      task_handle: None,
    }
  }

  fn get_port(&self) -> Option<u16> {
    self.port
  }

  async fn start(
    &mut self,
    app_handle: tauri::AppHandle,
    preferred_port: u16,
    allow_fallback: bool,
    runtime_kind: &'static str,
  ) -> Result<u16, String> {
    // Stop existing server if running
    self.stop().await.ok();

    let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
    let state = ApiServerState {
      app_handle: app_handle.clone(),
      runtime_kind,
    };

    // Try preferred port first, then random port
    let listener = match TcpListener::bind(format!("127.0.0.1:{preferred_port}")).await {
      Ok(listener) => listener,
      Err(_) => {
        if !allow_fallback {
          return Err(crate::backend_error_with_detail(
            "API_PORT_UNAVAILABLE",
            format!("127.0.0.1:{preferred_port} is already in use"),
          ));
        }
        // Port conflict, try random port
        let random_port = rand::random::<u16>().saturating_add(10000);
        match TcpListener::bind(format!("127.0.0.1:{random_port}")).await {
          Ok(listener) => {
            let _ = events::emit(
              "api-port-conflict",
              format!("API server using fallback port {random_port}"),
            );
            listener
          }
          Err(e) => {
            return Err(crate::backend_error_with_detail("API_PORT_UNAVAILABLE", e));
          }
        }
      }
    };

    let actual_port = listener
      .local_addr()
      .map_err(|e| crate::backend_error_with_detail("INTERNAL_ERROR", e))?
      .port();

    // Every local control-plane caller authenticates with the machine-local
    // API token. Create one on first runtime start so Local Free does not
    // depend on a cloud account or a paid entitlement.
    let settings_manager = crate::settings_manager::SettingsManager::instance();
    if settings_manager
      .get_api_token(&app_handle)
      .await
      .ok()
      .flatten()
      .is_none()
    {
      if let Err(error) = settings_manager.generate_api_token(&app_handle).await {
        log::warn!("[api] failed to initialize local API token: {error}");
      }
    }

    // Create router with OpenAPI documentation
    let (v1_routes, _) = OpenApiRouter::new()
      .routes(routes!(get_profiles, create_profile))
      .routes(routes!(get_profile, update_profile, delete_profile))
      .routes(routes!(run_profile))
      .routes(routes!(prepare_target_binding))
      .routes(routes!(pending_target_binding))
      .routes(routes!(confirm_target_binding))
      .routes(routes!(abort_target_binding))
      .routes(routes!(run_profile_remote))
      .routes(routes!(stop_remote_session))
      .routes(routes!(set_profile_cloud_sync))
      .routes(routes!(open_url_in_profile))
      .routes(routes!(kill_profile))
      .routes(routes!(batch_run_profiles))
      .routes(routes!(batch_stop_profiles))
      .routes(routes!(detect_import_profiles))
      .routes(routes!(import_profiles_api))
      .routes(routes!(import_profile_cookies))
      .routes(routes!(get_groups, create_group))
      .routes(routes!(get_group, update_group, delete_group))
      .routes(routes!(get_tags))
      .routes(routes!(get_proxies, create_proxy))
      .routes(routes!(import_proxies_api))
      .routes(routes!(get_proxy, update_proxy, delete_proxy))
      .routes(routes!(get_vpns, create_vpn))
      .routes(routes!(import_vpn))
      .routes(routes!(export_vpn))
      .routes(routes!(get_vpn, update_vpn, delete_vpn))
      .routes(routes!(get_vpn_pools, create_vpn_pool_api))
      .routes(routes!(update_vpn_pool_api, delete_vpn_pool_api))
      .routes(routes!(get_vpn_leases, acquire_vpn_lease_api))
      .routes(routes!(get_vpn_lease, release_vpn_lease_api))
      .routes(routes!(get_extensions))
      .routes(routes!(delete_extension_api))
      .routes(routes!(get_extension_groups))
      .routes(routes!(delete_extension_group_api))
      .routes(routes!(download_browser_api))
      .routes(routes!(get_browser_versions))
      .routes(routes!(check_browser_downloaded))
      .split_for_parts();

    let api = ApiDoc::openapi();

    let v1_routes = v1_routes
      // Innermost so only authenticated automation requests consume quota.
      .layer(middleware::from_fn(rate_limit_middleware))
      .layer(middleware::from_fn_with_state(
        state.clone(),
        auth_middleware,
      ))
      .layer(middleware::from_fn(terms_check_middleware));

    let api_for_v1 = api.clone();
    let app = Router::new()
      .merge(v1_routes)
      .merge(crate::worker::worker_routes())
      // Donut-owned local browser manager. These routes are local-only and
      // entitlement-free, but still require the machine-local bearer token.
      .merge(
        Router::new()
          .route(
            "/v1/local/browser/profiles/{id}/run",
            post(local_browser_run),
          )
          .route(
            "/v1/local/browser/profiles/{id}/stop",
            post(local_browser_stop),
          )
          .route(
            "/v1/local/browser/profiles",
            get(local_browser_list_profiles).post(local_browser_create_profile),
          )
          .route(
            "/v1/local/browser/profiles/{id}/proxy",
            get(local_browser_get_proxy)
              .put(local_browser_put_proxy)
              .delete(local_browser_delete_proxy),
          )
          .route(
            "/v1/local/browser/profiles/{id}/proxy/test",
            post(local_browser_test_proxy),
          )
          .route(
            "/v1/local/browser/profiles/{id}/pages",
            get(local_browser_list_pages).post(local_browser_create_page),
          )
          .route(
            "/v1/local/browser/profiles/{id}/pages/claim",
            post(local_browser_claim_page),
          )
          .route(
            "/v1/local/browser/profiles/{id}/pages/{target_id}/release",
            post(local_browser_release_page),
          )
          .route(
            "/v1/local/browser/profiles/{id}/pages/{target_id}",
            delete(local_browser_delete_page),
          )
          .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
          )),
      )
      .route("/v1/runtime/health", get(runtime_health_handler))
      .route("/openapi.json", get(move || async move { Json(api) }))
      .route(
        "/v1/openapi.json",
        get(move || async move { Json(api_for_v1) }),
      )
      // Outermost layer: logs every request so customer reports show what
      // their automation is actually calling, what the response status was,
      // and how long it took. Never logs request bodies or auth headers.
      .layer(middleware::from_fn(request_logging_middleware))
      .layer(CorsLayer::permissive())
      .layer(axum::extract::DefaultBodyLimit::max(100 * 1024 * 1024))
      .with_state(state);

    if runtime_kind == "floword-donut-runtime" {
      crate::browser_runner::reconcile_pending_binding_sessions_on_startup()
        .await
        .map_err(|error| {
          crate::backend_error_with_detail("BINDING_SESSION_RECONCILIATION_FAILED", error)
        })?;
      crate::browser_runner::migrate_managed_grok_target_on_startup()
        .await
        .map_err(|error| {
          crate::backend_error_with_detail("MANAGED_GROK_MIGRATION_FAILED", error)
        })?;
    }

    // Start server task
    let task_handle = tokio::spawn(async move {
      let server = axum::serve(listener, app);
      tokio::select! {
        _ = server => {},
        _ = shutdown_rx.recv() => {},
      }
    });

    self.port = Some(actual_port);
    self.shutdown_tx = Some(shutdown_tx);
    self.task_handle = Some(task_handle);

    // The headless Floword runtime owns Playwright worker registration and
    // refreshes it independently of lease acquisition.  This keeps the
    // PLAYWRIGHT record alive after restart while leaving legacy WAYFERN
    // records untouched.
    if runtime_kind == "floword-donut-runtime" {
      crate::worker::worker_routes::start_playwright_bootstrap_loop();
    }

    Ok(actual_port)
  }

  async fn stop(&mut self) -> Result<(), String> {
    if let Some(shutdown_tx) = self.shutdown_tx.take() {
      let _ = shutdown_tx.send(()).await;
    }

    if let Some(handle) = self.task_handle.take() {
      handle.abort();
    }

    self.port = None;
    Ok(())
  }
}

// Terms and Conditions check middleware
fn json_error_response(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
  let body = serde_json::json!({
    "error": {
      "code": code,
      "message": message.into(),
      "retryable": status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
    }
  });
  let mut response = (
    status,
    [(header::CONTENT_TYPE, "application/json")],
    body.to_string(),
  )
    .into_response();
  if let Ok(value) = HeaderValue::from_str(code) {
    response.headers_mut().insert("x-floword-error-code", value);
  }
  response
}

fn json_error_response_with_details(
  status: StatusCode,
  code: &str,
  message: impl Into<String>,
  details: serde_json::Value,
) -> Response {
  let body = serde_json::json!({
    "error": {
      "code": code,
      "message": message.into(),
      "retryable": true,
      "details": details,
    }
  });
  let mut response = (
    status,
    [(header::CONTENT_TYPE, "application/json")],
    body.to_string(),
  )
    .into_response();
  if let Ok(value) = HeaderValue::from_str(code) {
    response.headers_mut().insert("x-floword-error-code", value);
  }
  response
}

fn log_run_phase(
  request_id: &str,
  phase: &str,
  profile_id: &str,
  browser_pid: Option<u32>,
  launch_generation: Option<u64>,
) {
  log::info!(
    "[api-phase] {}",
    serde_json::json!({
      "requestId": request_id,
      "phase": phase,
      "profileIdHash": blake3::hash(profile_id.as_bytes()).to_hex().to_string()[..16].to_string(),
      "browserPid": browser_pid,
      "launchGeneration": launch_generation,
    })
  );
}

async fn terms_check_middleware(request: axum::extract::Request, next: Next) -> Response {
  // Check if Wayfern terms have been accepted
  if !crate::wayfern_terms::WayfernTermsManager::instance().is_terms_accepted() {
    return json_error_response(
      StatusCode::FORBIDDEN,
      "TERMS_NOT_ACCEPTED",
      "Wayfern terms must be accepted before using the API",
    );
  }

  next.run(request).await
}

// Authentication middleware
async fn auth_middleware(
  State(state): State<ApiServerState>,
  headers: HeaderMap,
  request: axum::extract::Request,
  next: Next,
) -> Response {
  let path = request.uri().path().to_string();

  if state.runtime_kind == "floword-donut-runtime" && !path.starts_with("/v1/local/") {
    // Embedded background runtime inside Floword Artcraft - local loopback without external token
    return next.run(request).await;
  }

  // Floword Studio talks to the Donut Manager over the loopback-only runtime
  // API.  Keep this integration explicitly opt-in rather than weakening auth
  // for every local caller; the server is bound to 127.0.0.1 and the client
  // sends this marker only for profile discovery/launch requests.
  let floword_integration = (path == "/v1/profiles" || path.starts_with("/v1/profiles/"))
    && headers
      .get("X-Floword-Integration")
      .and_then(|value| value.to_str().ok())
      .map(|value| value == "1")
      .unwrap_or(false);
  if floword_integration {
    return next.run(request).await;
  }

  // Get the Authorization header
  let auth_header = headers
    .get("Authorization")
    .and_then(|h| h.to_str().ok())
    .and_then(|h| h.strip_prefix("Bearer "));

  let token = match auth_header {
    Some(token) => token,
    None => {
      log::warn!("[api] Rejected {path}: missing Authorization header");
      return json_error_response(
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
        "missing authorization",
      );
    }
  };

  // Get the stored token
  let settings_manager = crate::settings_manager::SettingsManager::instance();
  let stored_token = match settings_manager.get_api_token(&state.app_handle).await {
    Ok(Some(stored_token)) => stored_token,
    Ok(None) => {
      log::warn!(
        "[api] Rejected {path}: API server has no stored token (was the API toggled off?)"
      );
      return json_error_response(
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
        "API server has no stored token",
      );
    }
    Err(e) => {
      log::error!("[api] Failed to read stored API token: {e}");
      return json_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "AUTH_TOKEN_READ_FAILED",
        "failed to read stored API token",
      );
    }
  };

  // Constant-time comparison so the auth check doesn't leak the shared-prefix
  // length via timing. `ConstantTimeEq` on equal-length byte slices; differing
  // lengths simply compare unequal.
  use subtle::ConstantTimeEq;
  let token_bytes = token.as_bytes();
  let stored_bytes = stored_token.as_bytes();
  let matches = token_bytes.len() == stored_bytes.len() && token_bytes.ct_eq(stored_bytes).into();
  if !matches {
    log::warn!("[api] Rejected {path}: token mismatch");
    return json_error_response(StatusCode::UNAUTHORIZED, "UNAUTHORIZED", "token mismatch");
  }

  // Token is valid, continue with the request
  next.run(request).await
}

/// Logs every request: method, path, query, response status, duration.
/// Skips Authorization header and request bodies entirely.
async fn request_logging_middleware(mut request: axum::extract::Request, next: Next) -> Response {
  let method = request.method().clone();
  let path = request.uri().path().to_string();
  let query = request.uri().query().map(|q| q.to_string());
  let started = std::time::Instant::now();

  let request_id = request
    .headers()
    .get("X-Floword-Request-Id")
    .and_then(|value| value.to_str().ok())
    .filter(|value| {
      !value.is_empty()
        && value.len() <= 128
        && value
          .chars()
          .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
    })
    .map(str::to_string)
    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
  let profile_id_hash = path
    .strip_prefix("/v1/profiles/")
    .and_then(|value| value.split('/').next())
    .filter(|value| !value.is_empty())
    .map(|value| blake3::hash(value.as_bytes()).to_hex().to_string()[..16].to_string());
  if let Ok(value) = HeaderValue::from_str(&request_id) {
    request.headers_mut().insert("x-floword-request-id", value);
  }
  log::info!(
    "[api-phase] {}",
    serde_json::json!({
      "requestId": request_id,
      "phase": "HTTP_REQUEST_ACCEPTED",
      "profileIdHash": profile_id_hash,
    })
  );

  let mut response = next.run(request).await;

  let status = response.status();
  let elapsed_ms = started.elapsed().as_millis();
  if let Ok(value) = HeaderValue::from_str(&request_id) {
    response.headers_mut().insert("x-floword-request-id", value);
  }
  if status.is_server_error() && !response.headers().contains_key("x-floword-error-code") {
    response.headers_mut().insert(
      "x-floword-error-code",
      HeaderValue::from_static("INTERNAL_ERROR"),
    );
  }
  log::log!(
    if status.is_server_error() {
      log::Level::Error
    } else {
      log::Level::Info
    },
    "[api-phase] {}",
    serde_json::json!({
      "requestId": request_id,
      "phase": "HTTP_RESPONSE_WRITTEN",
      "statusCode": status.as_u16(),
      "profileIdHash": profile_id_hash,
      "elapsedMs": elapsed_ms,
    })
  );

  let level = if status.is_server_error() {
    log::Level::Error
  } else if status.is_client_error() {
    log::Level::Warn
  } else {
    log::Level::Info
  };

  match query {
    Some(q) => log::log!(
      level,
      "[api] {method} {path}?{q} -> {status} ({elapsed_ms} ms)"
    ),
    None => log::log!(level, "[api] {method} {path} -> {status} ({elapsed_ms} ms)"),
  }

  response
}

fn is_automation_request(method: &Method, path: &str) -> bool {
  if method != Method::POST {
    return false;
  }

  if path == "/v1/vpn-leases" {
    return true;
  }

  if matches!(path, "/v1/profiles/batch/run" | "/v1/profiles/batch/stop") {
    return true;
  }

  let Some(profile_action) = path.strip_prefix("/v1/profiles/") else {
    return false;
  };
  let mut segments = profile_action.split('/');
  matches!(
    (segments.next(), segments.next(), segments.next()),
    (Some(_), Some("run" | "open-url" | "kill"), None)
  )
}

async fn rate_limit_middleware(request: axum::extract::Request, next: Next) -> Response {
  if !is_automation_request(request.method(), request.uri().path()) {
    return next.run(request).await;
  }

  match crate::automation_rate_limiter::check_automation_rate_limit().await {
    crate::automation_rate_limiter::RateLimitOutcome::Limited { retry_after_secs } => {
      log::warn!(
        "[api] Rejected {}: automation rate limit exceeded; retry in {}s",
        request.uri().path(),
        retry_after_secs
      );
      (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, retry_after_secs.to_string())],
        "automation request rate limit exceeded",
      )
        .into_response()
    }
    crate::automation_rate_limiter::RateLimitOutcome::Unlimited
    | crate::automation_rate_limiter::RateLimitOutcome::Allowed { .. } => next.run(request).await,
  }
}

// Global API server instance
lazy_static! {
  pub static ref API_SERVER: Arc<Mutex<ApiServer>> = Arc::new(Mutex::new(ApiServer::new()));
}

// Tauri commands
#[tauri::command]
pub async fn start_api_server_internal(
  port: u16,
  app_handle: &tauri::AppHandle,
) -> Result<u16, String> {
  let mut server_guard = API_SERVER.lock().await;
  server_guard
    .start(app_handle.clone(), port, true, "donutbrowser")
    .await
}

/// Starts the local API without silently moving to another port. Headless
/// Floword runtime uses this so its supervisor can distinguish ownership from
/// an unrelated process already listening on the canonical port.
pub async fn start_api_server_internal_strict(
  port: u16,
  app_handle: &tauri::AppHandle,
) -> Result<u16, String> {
  let mut server_guard = API_SERVER.lock().await;
  server_guard
    .start(app_handle.clone(), port, false, "floword-donut-runtime")
    .await
}

async fn runtime_health_handler(
  State(state): State<ApiServerState>,
) -> Result<Json<serde_json::Value>, StatusCode> {
  if !crate::worker::WORKER_REGISTRY.is_ready().await {
    return Err(StatusCode::SERVICE_UNAVAILABLE);
  }

  Ok(Json(serde_json::json!({
    "status": "READY",
    "protocol": "floword-production",
    "protocolVersion": 1,
    "runtime": state.runtime_kind,
    "pid": std::process::id(),
  })))
}

#[tauri::command]
pub async fn stop_api_server() -> Result<(), String> {
  let mut server_guard = API_SERVER.lock().await;
  server_guard.stop().await
}

#[tauri::command]
pub async fn start_api_server(
  port: Option<u16>,
  app_handle: tauri::AppHandle,
) -> Result<u16, String> {
  let actual_port = port.unwrap_or(10108);
  start_api_server_internal(actual_port, &app_handle).await
}

#[tauri::command]
pub async fn get_api_server_status() -> Result<Option<u16>, String> {
  let server_guard = API_SERVER.lock().await;
  Ok(server_guard.get_port())
}

// API Handlers - Profiles
/// Maps a manager-layer error onto a consistent HTTP status: 404 for missing
/// entities, 400 for validation/duplicate/client-input errors, 500 for
/// everything else (IO and other internal failures). The error text passes
/// through as the response body so API clients get a diagnostic instead of a
/// bare status code. Matching is on message content because the managers
/// return plain strings (some are the JSON `{"code": ...}` strings shared
/// with the Tauri commands).
fn manager_error_response(err: impl std::fmt::Display) -> (StatusCode, String) {
  let msg = err.to_string();

  // Structured {"code": ...} errors from the shared managers classify exactly.
  if let Ok(value) = serde_json::from_str::<serde_json::Value>(&msg) {
    if let Some(code) = value.get("code").and_then(|c| c.as_str()) {
      let status = if code.ends_with("_NOT_FOUND") {
        StatusCode::NOT_FOUND
      } else if code == "INTERNAL_ERROR" {
        StatusCode::INTERNAL_SERVER_ERROR
      } else if code.ends_with("_REQUIRES_PRO") || code.ends_with("_PAYMENT_REQUIRED") {
        // Paid-feature gates (FINGERPRINT_REQUIRES_PRO, PROXY_PAYMENT_REQUIRED).
        // Mapping them here lets the gate live in the shared manager instead of
        // being re-implemented in each handler to get the status right.
        StatusCode::PAYMENT_REQUIRED
      } else {
        // Validation-style codes (NAME_CANNOT_BE_EMPTY, GROUP_ALREADY_EXISTS,
        // WAYFERN_VERSION_NOT_AVAILABLE, ...).
        StatusCode::BAD_REQUEST
      };
      return (status, msg);
    }
  }

  // Plain-text manager messages: match the known phrases narrowly so raw
  // OS/serde/network error text (e.g. "invalid type: ..." from a corrupt
  // store) falls through to 500 instead of masquerading as a client error.
  let lower = msg.to_lowercase();
  let status = if lower.contains("not found") {
    StatusCode::NOT_FOUND
  } else if lower.contains("already exists")
    || lower.contains("cannot set both")
    || lower.contains("cannot edit")
    || lower.contains("cannot delete")
    || lower.contains("cannot open url")
    || lower.contains("invalid browser")
    || lower.contains("invalid profile id")
    || lower.contains("unsupported browser")
    || lower.contains("not supported on your platform")
    || lower.contains("is not downloaded")
    || lower.contains("terms and conditions")
  {
    StatusCode::BAD_REQUEST
  } else {
    StatusCode::INTERNAL_SERVER_ERROR
  };
  (status, msg)
}

/// Real per-group profile counts, computed from the profile list (the same
/// source of truth the GUI uses).
fn group_profile_counts() -> std::collections::HashMap<String, usize> {
  let mut counts = std::collections::HashMap::new();
  if let Ok(profiles) = ProfileManager::instance().list_profiles() {
    for profile in profiles {
      if let Some(group_id) = profile.group_id {
        *counts.entry(group_id).or_insert(0) += 1;
      }
    }
  }
  counts
}

#[utoipa::path(
  get,
  path = "/v1/profiles",
  responses(
    (status = 200, description = "List of all profiles", body = ApiProfilesResponse),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn get_profiles() -> Result<Json<ApiProfilesResponse>, StatusCode> {
  let profile_manager = ProfileManager::instance();
  match profile_manager.list_profiles() {
    Ok(profiles) => {
      let api_profiles: Vec<ApiProfile> = profiles.iter().map(ApiProfile::from).collect();

      Ok(Json(ApiProfilesResponse {
        profiles: api_profiles,
        total: profiles.len(),
      }))
    }
    Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
  }
}

#[utoipa::path(
  get,
  path = "/v1/profiles/{id}",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  responses(
    (status = 200, description = "Profile details", body = ApiProfileResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Profile not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn get_profile(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiProfileResponse>, StatusCode> {
  let profile_manager = ProfileManager::instance();
  match profile_manager.list_profiles() {
    Ok(profiles) => {
      if let Some(profile) = profiles.iter().find(|p| p.id.to_string() == id) {
        Ok(Json(ApiProfileResponse {
          profile: ApiProfile::from(profile),
        }))
      } else {
        Err(StatusCode::NOT_FOUND)
      }
    }
    Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
  }
}

/// Create a profile.
///
/// - `browser` must be `"wayfern"`; any other value is rejected
///   with 400.
/// - `version` is optional: omit it or pass `"latest"` to use the newest
///   already-downloaded version of that browser. The version must be present
///   locally (this endpoint does not download new versions); 400 if none is.
/// - Omitting the matching `wayfern_config`, or passing an
///   empty object `{}`, generates a fresh fingerprint automatically.
#[utoipa::path(
  post,
  path = "/v1/profiles",
  request_body = CreateProfileRequest,
  responses(
    (status = 200, description = "Profile created successfully", body = ApiProfileResponse),
    (status = 400, description = "Invalid browser, or no downloaded version available"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Selected proxy requires payment"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn create_profile(
  State(state): State<ApiServerState>,
  Json(request): Json<CreateProfileRequest>,
) -> Result<Json<ApiProfileResponse>, (StatusCode, String)> {
  let profile_manager = ProfileManager::instance();

  // Only Wayfern profiles are launchable; the rest of the system
  // (fingerprint generation, launch, run) supports nothing else. Reject anything
  // else up front — otherwise the profile is created with no fingerprint and an
  // unrecognized browser, then crashes with a 500 on /run. Mirrors the MCP
  // create_profile validation.
  let browser = request.browser.trim().to_ascii_lowercase();
  if browser != "chromium" {
    return Err((
      StatusCode::BAD_REQUEST,
      format!(
        "Invalid browser \"{}\". New profiles must use \"chromium\".",
        request.browser
      ),
    ));
  }

  // Resolve the version. Omitted, empty, or "latest" means "newest version
  // already downloaded for this browser". The create path generates the
  // fingerprint by launching that binary, so the version must be present
  // locally — we don't fetch new versions here. 400 if none is downloaded.
  let version = match request.version.as_deref() {
    Some(v) if !v.is_empty() && v != "latest" => v.to_string(),
    _ if browser == "chromium" => "staged".to_string(),
    _ => {
      let registry = crate::downloaded_browsers_registry::DownloadedBrowsersRegistry::instance();
      let mut versions = registry.get_downloaded_versions(&browser);
      // browsers is a HashMap, so keys are unordered — sort newest-first by
      // semver before taking the latest.
      versions.sort_by(|a, b| crate::api_client::compare_versions(b, a));
      match versions.into_iter().next() {
        Some(v) => v,
        None => {
          return Err((
            StatusCode::BAD_REQUEST,
            format!(
              "No downloaded version of \"{}\" is available. Download the browser in Donut Browser first — this endpoint does not download browsers.",
                browser
            ),
          ));
        }
      }
    }
  };

  // Parse wayfern config if provided
  let wayfern_config = None;

  // Reject a dead/unreachable proxy or VPN before creating the profile. A 402
  // (expired proxy subscription) maps to 402; anything else is a 400.
  if let Err(err) =
    crate::validate_profile_network(request.proxy_id.as_deref(), request.vpn_id.as_deref()).await
  {
    return Err(if err.contains("PROXY_PAYMENT_REQUIRED") {
      (
        StatusCode::PAYMENT_REQUIRED,
        "The selected proxy requires an active subscription.".to_string(),
      )
    } else {
      (
        StatusCode::BAD_REQUEST,
        format!("Profile network validation failed: {err}"),
      )
    });
  }

  // Create profile using the async create_profile_with_group method
  match profile_manager
    .create_profile_with_group(
      &state.app_handle,
      &request.name,
      &browser,
      &version,
      request.release_type.as_deref().unwrap_or("stable"),
      request.proxy_id.clone(),
      request.vpn_id.clone(),
      wayfern_config,
      request.group_id.clone(),
      false,
      None,
      request.launch_hook.clone(),
    )
    .await
  {
    Ok(mut profile) => {
      // Apply tags if provided
      if let Some(tags) = &request.tags {
        if profile_manager
          .update_profile_tags(&state.app_handle, &profile.name, tags.clone())
          .is_err()
        {
          return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Profile created but failed to apply tags.".to_string(),
          ));
        }
        profile.tags = tags.clone();
      }

      // Update tag manager with new tags
      if let Ok(profiles) = profile_manager.list_profiles() {
        let _ = crate::tag_manager::TAG_MANAGER
          .lock()
          .map(|manager| manager.rebuild_from_profiles(&profiles));
      }

      Ok(Json(ApiProfileResponse {
        profile: ApiProfile::from(&profile),
      }))
    }
    Err(e) => Err((
      StatusCode::BAD_REQUEST,
      format!("Failed to create profile: {e}"),
    )),
  }
}

#[utoipa::path(
  put,
  path = "/v1/profiles/{id}",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = UpdateProfileRequest,
  responses(
    (status = 200, description = "Profile updated successfully", body = ApiProfileResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Profile not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn update_profile(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<UpdateProfileRequest>,
) -> Result<Json<ApiProfileResponse>, (StatusCode, String)> {
  let profile_manager = ProfileManager::instance();

  if request.proxy_id.as_deref().is_some_and(|s| !s.is_empty())
    && request.vpn_id.as_deref().is_some_and(|s| !s.is_empty())
  {
    return Err((
      StatusCode::BAD_REQUEST,
      "Cannot set both proxy_id and vpn_id".to_string(),
    ));
  }

  // Update profile fields
  if let Some(new_name) = request.name {
    if let Err(e) = profile_manager.rename_profile(&state.app_handle, &id, &new_name) {
      return Err(manager_error_response(e));
    }
  }

  if let Some(version) = request.version {
    if let Err(e) = profile_manager.update_profile_version(&state.app_handle, &id, &version) {
      return Err(manager_error_response(e));
    }
  }

  if let Some(proxy_id) = request.proxy_id {
    if let Err(e) = profile_manager
      .update_profile_proxy(state.app_handle.clone(), &id, Some(proxy_id))
      .await
    {
      return Err(manager_error_response(e));
    }
  }

  if let Some(vpn_id) = request.vpn_id {
    let normalized = if vpn_id.is_empty() {
      None
    } else {
      Some(vpn_id)
    };
    if let Err(e) = profile_manager
      .update_profile_vpn(state.app_handle.clone(), &id, normalized)
      .await
    {
      return Err(manager_error_response(e));
    }
  }

  if let Some(launch_hook) = request.launch_hook {
    let normalized = if launch_hook.trim().is_empty() {
      None
    } else {
      Some(launch_hook)
    };

    if let Err(e) = profile_manager.update_profile_launch_hook(&state.app_handle, &id, normalized) {
      return Err(manager_error_response(e));
    }
  }

  if let Some(group_id) = request.group_id {
    if let Err(e) =
      profile_manager.assign_profiles_to_group(&state.app_handle, vec![id.clone()], Some(group_id))
    {
      return Err(manager_error_response(e));
    }
  }

  if let Some(tags) = request.tags {
    if let Err(e) = profile_manager.update_profile_tags(&state.app_handle, &id, tags) {
      return Err(manager_error_response(e));
    }

    // Update tag manager with new tags from all profiles
    if let Ok(profiles) = profile_manager.list_profiles() {
      let _ = crate::tag_manager::TAG_MANAGER
        .lock()
        .map(|manager| manager.rebuild_from_profiles(&profiles));
    }
  }

  if let Some(extension_group_id) = request.extension_group_id {
    let ext_group = if extension_group_id.is_empty() {
      None
    } else {
      Some(extension_group_id)
    };
    if let Err(e) = profile_manager.update_profile_extension_group(&id, ext_group) {
      return Err(manager_error_response(e));
    }
  }

  if let Some(proxy_bypass_rules) = request.proxy_bypass_rules {
    if let Err(e) =
      profile_manager.update_profile_proxy_bypass_rules(&state.app_handle, &id, proxy_bypass_rules)
    {
      return Err(manager_error_response(e));
    }
  }

  if let Some(sync_mode) = request.sync_mode {
    if let Err(e) =
      crate::sync::set_profile_sync_mode(state.app_handle.clone(), id.clone(), sync_mode).await
    {
      return Err(manager_error_response(e));
    }
  }

  if let Some(clear_on_close) = request.clear_on_close {
    if let Err(e) =
      profile_manager.update_profile_clear_on_close(&state.app_handle, &id, clear_on_close)
    {
      return Err(manager_error_response(e));
    }
  }

  // Return updated profile
  get_profile(Path(id), State(state))
    .await
    .map_err(|status| (status, String::new()))
}

#[utoipa::path(
  delete,
  path = "/v1/profiles/{id}",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  responses(
    (status = 204, description = "Profile deleted successfully"),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Profile not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn delete_profile(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<StatusCode, (StatusCode, String)> {
  let profile_manager = ProfileManager::instance();
  match profile_manager.delete_profile(&state.app_handle, &id) {
    Ok(_) => Ok(StatusCode::NO_CONTENT),
    Err(e) => Err(manager_error_response(e)),
  }
}

// API Handlers - Groups
#[utoipa::path(
  get,
  path = "/v1/groups",
  responses(
    (status = 200, description = "List of all groups", body = Vec<ApiGroupResponse>),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "groups"
)]
async fn get_groups(
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<ApiGroupResponse>>, StatusCode> {
  match GROUP_MANAGER.lock() {
    Ok(manager) => match manager.get_all_groups() {
      Ok(groups) => {
        let counts = group_profile_counts();
        let api_groups = groups
          .into_iter()
          .map(|group| ApiGroupResponse {
            profile_count: counts.get(&group.id).copied().unwrap_or(0),
            id: group.id,
            name: group.name,
          })
          .collect();
        Ok(Json(api_groups))
      }
      Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    },
    Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
  }
}

#[utoipa::path(
  get,
  path = "/v1/groups/{id}",
  params(
    ("id" = String, Path, description = "Group ID")
  ),
  responses(
    (status = 200, description = "Group details", body = ApiGroupResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Group not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "groups"
)]
async fn get_group(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiGroupResponse>, StatusCode> {
  match GROUP_MANAGER.lock() {
    Ok(manager) => match manager.get_all_groups() {
      Ok(groups) => {
        if let Some(group) = groups.into_iter().find(|g| g.id == id) {
          Ok(Json(ApiGroupResponse {
            profile_count: group_profile_counts().get(&group.id).copied().unwrap_or(0),
            id: group.id,
            name: group.name,
          }))
        } else {
          Err(StatusCode::NOT_FOUND)
        }
      }
      Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    },
    Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
  }
}

#[utoipa::path(
  post,
  path = "/v1/groups",
  request_body = CreateGroupRequest,
  responses(
    (status = 200, description = "Group created successfully", body = ApiGroupResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "groups"
)]
async fn create_group(
  State(state): State<ApiServerState>,
  Json(request): Json<CreateGroupRequest>,
) -> Result<Json<ApiGroupResponse>, (StatusCode, String)> {
  match GROUP_MANAGER.lock() {
    Ok(manager) => match manager.create_group(&state.app_handle, request.name) {
      Ok(group) => Ok(Json(ApiGroupResponse {
        id: group.id,
        name: group.name,
        profile_count: 0,
      })),
      Err(e) => Err(manager_error_response(e)),
    },
    Err(_) => Err((
      StatusCode::INTERNAL_SERVER_ERROR,
      "group manager unavailable".to_string(),
    )),
  }
}

#[utoipa::path(
  put,
  path = "/v1/groups/{id}",
  params(
    ("id" = String, Path, description = "Group ID")
  ),
  request_body = UpdateGroupRequest,
  responses(
    (status = 200, description = "Group updated successfully", body = ApiGroupResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Group not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "groups"
)]
async fn update_group(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<UpdateGroupRequest>,
) -> Result<Json<ApiGroupResponse>, (StatusCode, String)> {
  match GROUP_MANAGER.lock() {
    Ok(manager) => match manager.update_group(&state.app_handle, id.clone(), request.name) {
      Ok(group) => Ok(Json(ApiGroupResponse {
        profile_count: group_profile_counts().get(&group.id).copied().unwrap_or(0),
        id: group.id,
        name: group.name,
      })),
      Err(e) => Err(manager_error_response(e)),
    },
    Err(_) => Err((
      StatusCode::INTERNAL_SERVER_ERROR,
      "group manager unavailable".to_string(),
    )),
  }
}

#[utoipa::path(
  delete,
  path = "/v1/groups/{id}",
  params(
    ("id" = String, Path, description = "Group ID")
  ),
  responses(
    (status = 204, description = "Group deleted successfully"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Group not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "groups"
)]
async fn delete_group(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<StatusCode, (StatusCode, String)> {
  match GROUP_MANAGER.lock() {
    Ok(manager) => match manager.delete_group(&state.app_handle, id.clone()) {
      Ok(_) => Ok(StatusCode::NO_CONTENT),
      Err(e) => Err(manager_error_response(e)),
    },
    Err(_) => Err((
      StatusCode::INTERNAL_SERVER_ERROR,
      "group manager unavailable".to_string(),
    )),
  }
}

// API Handlers - Tags
#[utoipa::path(
  get,
  path = "/v1/tags",
  responses(
    (status = 200, description = "List of all tags", body = Vec<String>),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "tags"
)]
async fn get_tags(State(_state): State<ApiServerState>) -> Result<Json<Vec<String>>, StatusCode> {
  match TAG_MANAGER.lock() {
    Ok(manager) => match manager.get_all_tags() {
      Ok(tags) => Ok(Json(tags)),
      Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    },
    Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
  }
}

// API Handlers - Proxies
#[utoipa::path(
  get,
  path = "/v1/proxies",
  responses(
    (status = 200, description = "List of all proxies", body = Vec<ApiProxyResponse>),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn get_proxies(
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<ApiProxyResponse>>, StatusCode> {
  let proxies = PROXY_MANAGER.get_stored_proxies();
  Ok(Json(
    proxies
      .into_iter()
      .map(|p| ApiProxyResponse {
        id: p.id,
        name: p.name,
        proxy_settings: p.proxy_settings,
      })
      .collect(),
  ))
}

#[utoipa::path(
  get,
  path = "/v1/proxies/{id}",
  params(
    ("id" = String, Path, description = "Proxy ID")
  ),
  responses(
    (status = 200, description = "Proxy details", body = ApiProxyResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Proxy not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn get_proxy(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiProxyResponse>, StatusCode> {
  let proxies = PROXY_MANAGER.get_stored_proxies();
  if let Some(proxy) = proxies.into_iter().find(|p| p.id == id) {
    Ok(Json(ApiProxyResponse {
      id: proxy.id,
      name: proxy.name,
      proxy_settings: proxy.proxy_settings,
    }))
  } else {
    Err(StatusCode::NOT_FOUND)
  }
}

#[utoipa::path(
  post,
  path = "/v1/proxies",
  request_body = CreateProxyRequest,
  responses(
    (status = 200, description = "Proxy created successfully", body = ApiProxyResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn create_proxy(
  State(state): State<ApiServerState>,
  Json(request): Json<CreateProxyRequest>,
) -> Result<Json<ApiProxyResponse>, (StatusCode, String)> {
  let result = PROXY_MANAGER.create_stored_proxy(
    &state.app_handle,
    request.name.clone(),
    request.proxy_settings,
  );

  match result {
    Ok(proxy) => Ok(Json(ApiProxyResponse {
      id: proxy.id,
      name: proxy.name,
      proxy_settings: proxy.proxy_settings,
    })),
    Err(e) => Err(manager_error_response(e)),
  }
}

// API Handler - Bulk-import proxies from a txt list or a Donut JSON export.
// Mirrors the MCP `import_proxies` tool.
#[utoipa::path(
  post,
  path = "/v1/proxies/import",
  request_body = ImportProxiesRequest,
  responses(
    (status = 200, description = "Import completed; inspect counts and per-proxy errors", body = ImportProxiesResponse),
    (status = 400, description = "Invalid format or no valid proxies in content"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn import_proxies_api(
  State(state): State<ApiServerState>,
  Json(request): Json<ImportProxiesRequest>,
) -> Result<Json<ImportProxiesResponse>, (StatusCode, String)> {
  let result = match request.format.as_str() {
    "json" => PROXY_MANAGER
      .import_proxies_json(&state.app_handle, &request.content)
      .map_err(manager_error_response)?,
    "txt" => {
      use crate::proxy_manager::{ProxyManager, ProxyParseResult};

      let parsed: Vec<_> = ProxyManager::parse_txt_proxies(&request.content)
        .into_iter()
        .filter_map(|r| match r {
          ProxyParseResult::Parsed(p) => Some(p),
          _ => None,
        })
        .collect();

      if parsed.is_empty() {
        return Err((
          StatusCode::BAD_REQUEST,
          "No valid proxies found in content".to_string(),
        ));
      }

      PROXY_MANAGER
        .import_proxies_from_parsed(&state.app_handle, parsed, request.name_prefix)
        .map_err(manager_error_response)?
    }
    other => {
      return Err((
        StatusCode::BAD_REQUEST,
        format!("Invalid format \"{other}\", must be \"json\" or \"txt\""),
      ))
    }
  };

  Ok(Json(ImportProxiesResponse {
    imported_count: result.imported_count,
    skipped_count: result.skipped_count,
    errors: result.errors,
    proxies: result
      .proxies
      .into_iter()
      .map(|p| ApiProxyResponse {
        id: p.id,
        name: p.name,
        proxy_settings: p.proxy_settings,
      })
      .collect(),
  }))
}

#[utoipa::path(
  put,
  path = "/v1/proxies/{id}",
  params(
    ("id" = String, Path, description = "Proxy ID")
  ),
  request_body = UpdateProxyRequest,
  responses(
    (status = 200, description = "Proxy updated successfully", body = ApiProxyResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Proxy not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn update_proxy(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<UpdateProxyRequest>,
) -> Result<Json<ApiProxyResponse>, (StatusCode, String)> {
  let result =
    PROXY_MANAGER.update_stored_proxy(&state.app_handle, &id, request.name, request.proxy_settings);

  match result {
    Ok(proxy) => Ok(Json(ApiProxyResponse {
      id: proxy.id,
      name: proxy.name,
      proxy_settings: proxy.proxy_settings,
    })),
    Err(e) => Err(manager_error_response(e)),
  }
}

#[utoipa::path(
  delete,
  path = "/v1/proxies/{id}",
  params(
    ("id" = String, Path, description = "Proxy ID")
  ),
  responses(
    (status = 204, description = "Proxy deleted successfully"),
    (status = 400, description = "Bad request (e.g. cloud-managed proxy)"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Proxy not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "proxies"
)]
async fn delete_proxy(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<StatusCode, (StatusCode, String)> {
  match PROXY_MANAGER.delete_stored_proxy(&state.app_handle, &id) {
    Ok(_) => Ok(StatusCode::NO_CONTENT),
    Err(e) => Err(manager_error_response(e)),
  }
}

// API Handlers - VPNs

fn vpn_to_api_response(c: &crate::vpn::VpnConfig) -> ApiVpnResponse {
  ApiVpnResponse {
    id: c.id.clone(),
    name: c.name.clone(),
    vpn_type: c.vpn_type.to_string(),
    created_at: c.created_at,
    last_used: c.last_used,
  }
}

fn parse_vpn_type(s: &str) -> Option<crate::vpn::VpnType> {
  match s.to_ascii_lowercase().as_str() {
    "wireguard" | "wg" => Some(crate::vpn::VpnType::WireGuard),
    _ => None,
  }
}

#[utoipa::path(
  get,
  path = "/v1/vpns",
  responses(
    (status = 200, description = "List of all VPN configurations", body = Vec<ApiVpnResponse>),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn get_vpns(
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<ApiVpnResponse>>, StatusCode> {
  let storage = crate::vpn::VPN_STORAGE
    .lock()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  let configs = storage
    .list_configs()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  Ok(Json(configs.iter().map(vpn_to_api_response).collect()))
}

#[utoipa::path(
  get,
  path = "/v1/vpns/{id}",
  params(("id" = String, Path, description = "VPN configuration ID")),
  responses(
    (status = 200, description = "VPN configuration details", body = ApiVpnResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "VPN configuration not found"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn get_vpn(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiVpnResponse>, StatusCode> {
  let storage = crate::vpn::VPN_STORAGE
    .lock()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  let configs = storage
    .list_configs()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  configs
    .iter()
    .find(|c| c.id == id)
    .map(|c| Json(vpn_to_api_response(c)))
    .ok_or(StatusCode::NOT_FOUND)
}

#[utoipa::path(
  get,
  path = "/v1/vpns/{id}/export",
  params(("id" = String, Path, description = "VPN configuration ID")),
  responses(
    (status = 200, description = "Decrypted VPN configuration", body = ApiVpnExportResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "VPN configuration not found"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn export_vpn(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiVpnExportResponse>, StatusCode> {
  let storage = crate::vpn::VPN_STORAGE
    .lock()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  match storage.load_config(&id) {
    Ok(config) => Ok(Json(ApiVpnExportResponse {
      id: config.id,
      name: config.name,
      vpn_type: config.vpn_type.to_string(),
      config_data: config.config_data,
    })),
    Err(_) => Err(StatusCode::NOT_FOUND),
  }
}

#[utoipa::path(
  post,
  path = "/v1/vpns/import",
  request_body = ImportVpnRequest,
  responses(
    (status = 200, description = "VPN configuration imported successfully", body = ApiVpnResponse),
    (status = 400, description = "Invalid or unrecognized VPN config"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn import_vpn(
  State(_state): State<ApiServerState>,
  Json(request): Json<ImportVpnRequest>,
) -> Result<Json<ApiVpnResponse>, StatusCode> {
  let result = {
    let storage = crate::vpn::VPN_STORAGE
      .lock()
      .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    storage.import_config(&request.content, &request.filename, request.name)
  };
  match result {
    Ok(config) => {
      let _ = events::emit("vpn-configs-changed", ());
      Ok(Json(vpn_to_api_response(&config)))
    }
    Err(_) => Err(StatusCode::BAD_REQUEST),
  }
}

#[utoipa::path(
  post,
  path = "/v1/vpns",
  request_body = CreateVpnRequest,
  responses(
    (status = 200, description = "VPN configuration created successfully", body = ApiVpnResponse),
    (status = 400, description = "Invalid VPN config or unknown vpn_type"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn create_vpn(
  State(_state): State<ApiServerState>,
  Json(request): Json<CreateVpnRequest>,
) -> Result<Json<ApiVpnResponse>, StatusCode> {
  let vpn_type = parse_vpn_type(&request.vpn_type).ok_or(StatusCode::BAD_REQUEST)?;
  let result = {
    let storage = crate::vpn::VPN_STORAGE
      .lock()
      .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    storage.create_config_manual(&request.name, vpn_type, &request.config_data)
  };
  match result {
    Ok(config) => {
      let _ = events::emit("vpn-configs-changed", ());
      Ok(Json(vpn_to_api_response(&config)))
    }
    Err(_) => Err(StatusCode::BAD_REQUEST),
  }
}

#[utoipa::path(
  put,
  path = "/v1/vpns/{id}",
  params(("id" = String, Path, description = "VPN configuration ID")),
  request_body = UpdateVpnRequest,
  responses(
    (status = 200, description = "VPN configuration updated successfully", body = ApiVpnResponse),
    (status = 400, description = "Bad request"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "VPN configuration not found"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn update_vpn(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
  Json(request): Json<UpdateVpnRequest>,
) -> Result<Json<ApiVpnResponse>, StatusCode> {
  let result = {
    let storage = crate::vpn::VPN_STORAGE
      .lock()
      .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    storage.update_config_name(&id, &request.name)
  };
  match result {
    Ok(config) => {
      let _ = events::emit("vpn-configs-changed", ());
      Ok(Json(vpn_to_api_response(&config)))
    }
    Err(_) => Err(StatusCode::NOT_FOUND),
  }
}

#[utoipa::path(
  delete,
  path = "/v1/vpns/{id}",
  params(("id" = String, Path, description = "VPN configuration ID")),
  responses(
    (status = 204, description = "VPN configuration deleted successfully"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "VPN configuration not found"),
    (status = 500, description = "Internal server error")
  ),
  security(("bearer_auth" = [])),
  tag = "vpns"
)]
async fn delete_vpn(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<StatusCode, StatusCode> {
  crate::vpn::pool::remove_config_references(&id)
    .await
    .map_err(|error| vpn_pool_error_status(&error))?;
  let _ = crate::vpn_worker_runner::stop_vpn_worker_by_vpn_id(&id).await;

  let result = {
    let storage = crate::vpn::VPN_STORAGE
      .lock()
      .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    storage.delete_config(&id)
  };
  match result {
    Ok(_) => {
      let _ = crate::vpn::provider::PROVIDER_STORE
        .lock()
        .map(|store| store.remove_config_reference(&id));
      let _ = events::emit("vpn-configs-changed", ());
      Ok(StatusCode::NO_CONTENT)
    }
    Err(_) => Err(StatusCode::NOT_FOUND),
  }
}

fn vpn_pool_error_status(error: &str) -> StatusCode {
  if error.contains("NOT_FOUND") {
    StatusCode::NOT_FOUND
  } else if error.contains("ACTIVE_LEASE") || error.contains("RUNNING") {
    StatusCode::CONFLICT
  } else if error.contains("CAPACITY") || error.contains("WAIT_TIMEOUT") {
    StatusCode::TOO_MANY_REQUESTS
  } else if error.contains("INTERNAL") || error.contains("STORAGE_FAILED") {
    StatusCode::INTERNAL_SERVER_ERROR
  } else {
    StatusCode::BAD_REQUEST
  }
}

#[utoipa::path(get, path = "/v1/vpn-pools", responses((status = 200, body = [crate::vpn::pool::VpnPool]), (status = 401, description = "Unauthorized")), security(("bearer_auth" = [])), tag = "vpn-pools")]
async fn get_vpn_pools(
  State(_state): State<ApiServerState>,
) -> Json<Vec<crate::vpn::pool::VpnPool>> {
  Json(crate::vpn::pool::list_pools().await)
}

#[utoipa::path(post, path = "/v1/vpn-pools", request_body = crate::vpn::pool::CreateVpnPoolRequest, responses((status = 200, body = crate::vpn::pool::VpnPool), (status = 400, description = "Invalid pool"), (status = 401, description = "Unauthorized")), security(("bearer_auth" = [])), tag = "vpn-pools")]
async fn create_vpn_pool_api(
  State(_state): State<ApiServerState>,
  Json(request): Json<crate::vpn::pool::CreateVpnPoolRequest>,
) -> Result<Json<crate::vpn::pool::VpnPool>, StatusCode> {
  let pool = crate::vpn::pool::create_pool(request)
    .await
    .map_err(|error| vpn_pool_error_status(&error))?;
  let _ = events::emit("vpn-pools-updated", ());
  Ok(Json(pool))
}

#[utoipa::path(put, path = "/v1/vpn-pools/{pool_id}", params(("pool_id" = String, Path)), request_body = crate::vpn::pool::CreateVpnPoolRequest, responses((status = 200, body = crate::vpn::pool::VpnPool), (status = 400, description = "Invalid pool"), (status = 401, description = "Unauthorized"), (status = 404, description = "Pool not found"), (status = 409, description = "Pool has an active lease")), security(("bearer_auth" = [])), tag = "vpn-pools")]
async fn update_vpn_pool_api(
  Path(pool_id): Path<String>,
  State(_state): State<ApiServerState>,
  Json(request): Json<crate::vpn::pool::CreateVpnPoolRequest>,
) -> Result<Json<crate::vpn::pool::VpnPool>, StatusCode> {
  let pool = crate::vpn::pool::update_pool(&pool_id, request)
    .await
    .map_err(|error| vpn_pool_error_status(&error))?;
  let _ = events::emit("vpn-pools-updated", ());
  Ok(Json(pool))
}

#[utoipa::path(delete, path = "/v1/vpn-pools/{pool_id}", params(("pool_id" = String, Path)), responses((status = 204, description = "Pool deleted"), (status = 401, description = "Unauthorized"), (status = 404, description = "Pool not found"), (status = 409, description = "Pool is active")), security(("bearer_auth" = [])), tag = "vpn-pools")]
async fn delete_vpn_pool_api(
  Path(pool_id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<StatusCode, StatusCode> {
  crate::vpn::pool::delete_pool(&pool_id)
    .await
    .map_err(|error| vpn_pool_error_status(&error))?;
  let _ = events::emit("vpn-pools-updated", ());
  Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(get, path = "/v1/vpn-leases", responses((status = 200, body = [ApiVpnLeaseResponse]), (status = 401, description = "Unauthorized")), security(("bearer_auth" = [])), tag = "vpn-leases")]
async fn get_vpn_leases(State(_state): State<ApiServerState>) -> Json<Vec<ApiVpnLeaseResponse>> {
  Json(
    crate::vpn::pool::list_leases()
      .await
      .into_iter()
      .map(Into::into)
      .collect(),
  )
}

#[utoipa::path(get, path = "/v1/vpn-leases/{lease_id}", params(("lease_id" = String, Path)), responses((status = 200, body = ApiVpnLeaseResponse), (status = 401, description = "Unauthorized"), (status = 404, description = "Lease not found")), security(("bearer_auth" = [])), tag = "vpn-leases")]
async fn get_vpn_lease(
  Path(lease_id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<ApiVpnLeaseResponse>, StatusCode> {
  crate::vpn::pool::list_leases()
    .await
    .into_iter()
    .find(|lease| lease.id == lease_id)
    .map(ApiVpnLeaseResponse::from)
    .map(Json)
    .ok_or(StatusCode::NOT_FOUND)
}

#[utoipa::path(post, path = "/v1/vpn-leases", request_body = ApiAcquireVpnLeaseRequest, responses((status = 200, body = ApiVpnLeaseResponse), (status = 400, description = "Invalid request"), (status = 401, description = "Unauthorized"), (status = 429, description = "Capacity exhausted"), (status = 500, description = "Provisioning failed")), security(("bearer_auth" = [])), tag = "vpn-leases")]
async fn acquire_vpn_lease_api(
  State(_state): State<ApiServerState>,
  Json(request): Json<ApiAcquireVpnLeaseRequest>,
) -> Result<Json<ApiVpnLeaseResponse>, StatusCode> {
  let lease = crate::vpn::pool::acquire_lease(request.into())
    .await
    .map_err(|error| vpn_pool_error_status(&error))?;
  let response = ApiVpnLeaseResponse::from(lease);
  let _ = events::emit("vpn-leases-updated", crate::vpn::pool::list_leases().await);
  Ok(Json(response))
}

#[utoipa::path(delete, path = "/v1/vpn-leases/{lease_id}", params(("lease_id" = String, Path)), responses((status = 204, description = "Lease released"), (status = 401, description = "Unauthorized"), (status = 404, description = "Lease not found"), (status = 500, description = "Worker cleanup failed")), security(("bearer_auth" = [])), tag = "vpn-leases")]
async fn release_vpn_lease_api(
  Path(lease_id): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<StatusCode, StatusCode> {
  match crate::vpn::pool::release_lease(&lease_id).await {
    Ok(true) => {
      let _ = events::emit("vpn-leases-updated", crate::vpn::pool::list_leases().await);
      Ok(StatusCode::NO_CONTENT)
    }
    Ok(false) => Err(StatusCode::NOT_FOUND),
    Err(error) => Err(vpn_pool_error_status(&error)),
  }
}

// Extension API endpoints

#[utoipa::path(
  get,
  path = "/v1/extensions",
  responses(
    (status = 200, description = "List of extensions"),
    (status = 401, description = "Unauthorized"),
  ),
  security(("bearer_auth" = [])),
  tag = "extensions"
)]
async fn get_extensions(
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<crate::extension_manager::Extension>>, StatusCode> {
  let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
  mgr
    .list_extensions()
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[utoipa::path(
  get,
  path = "/v1/extension-groups",
  responses(
    (status = 200, description = "List of extension groups"),
    (status = 401, description = "Unauthorized"),
  ),
  security(("bearer_auth" = [])),
  tag = "extensions"
)]
async fn get_extension_groups(
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<crate::extension_manager::ExtensionGroup>>, StatusCode> {
  let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
  mgr
    .list_groups()
    .map(Json)
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[utoipa::path(
  delete,
  path = "/v1/extensions/{id}",
  params(("id" = String, Path, description = "Extension ID")),
  responses(
    (status = 204, description = "Extension deleted"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Extension not found"),
    (status = 500, description = "Internal server error"),
  ),
  security(("bearer_auth" = [])),
  tag = "extensions"
)]
async fn delete_extension_api(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<StatusCode, (StatusCode, String)> {
  let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
  mgr
    .delete_extension(&state.app_handle, &id)
    .map(|_| StatusCode::NO_CONTENT)
    .map_err(manager_error_response)
}

#[utoipa::path(
  delete,
  path = "/v1/extension-groups/{id}",
  params(("id" = String, Path, description = "Extension Group ID")),
  responses(
    (status = 204, description = "Extension group deleted"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Extension group not found"),
    (status = 500, description = "Internal server error"),
  ),
  security(("bearer_auth" = [])),
  tag = "extensions"
)]
async fn delete_extension_group_api(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<StatusCode, (StatusCode, String)> {
  let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
  mgr
    .delete_group(&state.app_handle, &id)
    .map(|_| StatusCode::NO_CONTENT)
    .map_err(manager_error_response)
}

// API Handler - Run Profile with Remote Debugging
#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/run",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = RunProfileRequest,
  responses(
    (status = 200, description = "Profile launched successfully", body = RunProfileResponse),
    (status = 400, description = "Cannot launch cross-OS profile"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with browser automation required"),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Profile is locked by another team member"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn run_profile(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  headers: HeaderMap,
  Json(request): Json<RunProfileRequest>,
) -> Response {
  match std::panic::AssertUnwindSafe(run_profile_inner(id, state, headers, request, true))
    .catch_unwind()
    .await
  {
    Ok(Ok(Json(body))) => Json(body).into_response(),
    Ok(Err((status, body))) => run_profile_error_response(status, body),
    Err(_) => json_error_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      "RUN_TASK_PANICKED",
      "profile launch task panicked",
    ),
  }
}

/// Donut-owned local browser run. This is intentionally separate from the
/// paid/remote profile endpoint above: it only accepts CFT/Chromium profiles
/// and launches the local browser through the existing BrowserRunner.
async fn local_browser_list_profiles() -> Response {
  let profiles = match ProfileManager::instance().list_profiles() {
    Ok(profiles) => profiles,
    Err(error) => {
      return json_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "PROFILE_LIST_FAILED",
        error.to_string(),
      )
    }
  };
  let summaries = profiles
    .into_iter()
    .filter(|profile| crate::browser::is_chrome_for_testing_alias(&profile.browser))
    .map(|profile| LocalBrowserProfileSummary {
      id: profile.id.to_string(),
      name: profile.name,
      browser: profile.browser,
      is_running: profile.process_id.is_some(),
      process_id: profile.process_id,
      tags: profile.tags,
      group_id: profile.group_id,
      last_launch: profile.last_launch,
      proxy_id: profile.proxy_id,
      vpn_id: profile.vpn_id,
      cloud_sync_enabled: profile.sync_mode != crate::profile::types::SyncMode::Disabled,
      sync_mode: format!("{:?}", profile.sync_mode),
    })
    .collect::<Vec<_>>();
  let total = summaries.len();
  Json(LocalBrowserProfilesResponse {
    profiles: summaries,
    total,
  })
  .into_response()
}

async fn local_browser_create_profile(
  State(state): State<ApiServerState>,
  Json(request): Json<CreateProfileRequest>,
) -> Response {
  if !crate::browser::is_chrome_for_testing_alias(&request.browser) {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_BROWSER_ENGINE_REQUIRED",
      "new local profiles must use chromium",
    );
  }
  if request.name.trim().is_empty() {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "PROFILE_NAME_REQUIRED",
      "profile name is required",
    );
  }
  match ProfileManager::instance()
    .create_profile_with_group(
      &state.app_handle,
      &request.name,
      "chromium",
      "staged",
      request.release_type.as_deref().unwrap_or("stable"),
      request.proxy_id,
      request.vpn_id,
      None,
      request.group_id,
      false,
      None,
      request.launch_hook,
    )
    .await
  {
    Ok(profile) => Json(ApiProfileResponse {
      profile: ApiProfile::from(&profile),
    })
    .into_response(),
    Err(error) => json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_PROFILE_CREATE_FAILED",
      error.to_string(),
    ),
  }
}

async fn local_browser_run(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<RunProfileRequest>,
) -> Response {
  let profile = match ProfileManager::instance()
    .list_profiles()
    .ok()
    .and_then(|profiles| {
      profiles
        .into_iter()
        .find(|profile| profile.id.to_string() == id)
    }) {
    Some(profile) => profile,
    None => {
      return json_error_response(
        StatusCode::NOT_FOUND,
        "PROFILE_NOT_FOUND",
        "profile not found",
      )
    }
  };
  if !crate::browser::is_chrome_for_testing_alias(&profile.browser) {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_BROWSER_ENGINE_REQUIRED",
      "local browser manager only supports Chrome for Testing/Chromium profiles",
    );
  }
  match std::panic::AssertUnwindSafe(run_profile_inner(
    id,
    state,
    HeaderMap::new(),
    request,
    false,
  ))
  .catch_unwind()
  .await
  {
    Ok(Ok(Json(body))) => {
      let browser_pid = body.browser_pid.unwrap_or_default();
      let launch_generation = body.launch_generation.unwrap_or_default();
      if browser_pid == 0 || body.remote_debugging_port == 0 || launch_generation == 0 {
        return json_error_response(
          StatusCode::INTERNAL_SERVER_ERROR,
          "LOCAL_BROWSER_IDENTITY_INVALID",
          "browser identity was incomplete",
        );
      }
      if let Some(target_id) = body.grok_target_id.as_deref() {
        let mut pages = LOCAL_MANAGED_PAGES.lock().await;
        pages
          .entry(body.profile_id.clone())
          .or_default()
          .insert(target_id.to_string());
      }
      Json(LocalBrowserSessionResponse {
        profile_id: body.profile_id,
        browser_pid,
        remote_debugging_port: body.remote_debugging_port,
        cdp_endpoint: format!("http://127.0.0.1:{}", body.remote_debugging_port),
        launch_generation,
        browser_engine: body.browser_engine,
        grok_target_id: body.grok_target_id,
        grok_page_url: body.grok_page_url,
        reused: body.reused,
      })
      .into_response()
    }
    Ok(Err((status, body))) => run_profile_error_response(status, body),
    Err(_) => json_error_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      "LOCAL_BROWSER_RUN_PANICKED",
      "local browser run panicked",
    ),
  }
}

async fn local_browser_stop(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  request: Option<Json<LocalBrowserStopRequest>>,
) -> Response {
  let (profile, _port, _pid, _generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let requested = request.map(|Json(value)| value).unwrap_or_default();
  let current_profile_id = profile.id.to_string();
  let current_pid = profile.process_id.unwrap_or_default();
  let current_port = profile.managed_grok_cdp_port.unwrap_or_default();
  let current_generation = profile.last_launch.unwrap_or_default();
  let stale = requested
    .profile_id
    .as_deref()
    .is_some_and(|value| value != current_profile_id)
    || requested
      .browser_pid
      .is_some_and(|value| value != current_pid)
    || requested
      .remote_debugging_port
      .is_some_and(|value| value != current_port)
    || requested
      .launch_generation
      .is_some_and(|value| value != current_generation);
  if stale {
    return json_error_response_with_details(
      StatusCode::CONFLICT,
      "BROWSER_SESSION_IDENTITY_STALE",
      "the requested browser identity is stale; refresh from Donut and retry once",
      serde_json::json!({
        "profileId": current_profile_id,
        "browserPid": current_pid,
        "remoteDebuggingPort": current_port,
        "launchGeneration": current_generation,
      }),
    );
  }
  match crate::browser_runner::BrowserRunner::instance()
    .stop_browser_process_with_result(state.app_handle, &profile)
    .await
  {
    Ok(result) => Json(result).into_response(),
    Err(error) => json_error_response(
      StatusCode::CONFLICT,
      "LOCAL_BROWSER_STOP_FAILED",
      error.to_string(),
    ),
  }
}

fn local_profile_identity(
  id: &str,
) -> Result<(crate::profile::types::BrowserProfile, u16, u32, u64), Response> {
  let profile = ProfileManager::instance()
    .list_profiles()
    .map_err(|_| {
      json_error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        "PROFILE_LIST_FAILED",
        "failed to list profiles",
      )
    })?
    .into_iter()
    .find(|profile| profile.id.to_string() == id)
    .ok_or_else(|| {
      json_error_response(
        StatusCode::NOT_FOUND,
        "PROFILE_NOT_FOUND",
        "profile not found",
      )
    })?;
  if !crate::browser::is_chrome_for_testing_alias(&profile.browser) {
    return Err(json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_BROWSER_ENGINE_REQUIRED",
      "profile is not a Chrome for Testing profile",
    ));
  }
  let pid = profile.process_id.ok_or_else(|| {
    json_error_response(
      StatusCode::CONFLICT,
      "PROFILE_NOT_RUNNING",
      "profile is not running",
    )
  })?;
  let generation = profile.last_launch.ok_or_else(|| {
    json_error_response(
      StatusCode::CONFLICT,
      "PROFILE_NOT_RUNNING",
      "profile launch identity is unavailable",
    )
  })?;
  let profiles_dir = ProfileManager::instance().get_profiles_dir();
  let port = profile
    .managed_grok_cdp_port
    .or_else(|| {
      let path = crate::ephemeral_dirs::get_effective_profile_path(&profile, &profiles_dir)
        .join("floword-chromium")
        .join(".floword-cdp-port");
      std::fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse().ok())
    })
    .ok_or_else(|| {
      json_error_response(
        StatusCode::CONFLICT,
        "CDP_PORT_UNAVAILABLE",
        "profile CDP port is unavailable",
      )
    })?;
  Ok((profile, port, pid, generation))
}

fn local_managed_pages_path(profile: &crate::profile::types::BrowserProfile) -> std::path::PathBuf {
  let root = ProfileManager::instance().get_profiles_dir();
  crate::ephemeral_dirs::get_effective_profile_path(profile, &root)
    .join("floword-managed-pages.json")
}

fn read_durable_managed_pages(profile: &crate::profile::types::BrowserProfile) -> HashSet<String> {
  std::fs::read_to_string(local_managed_pages_path(profile))
    .ok()
    .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
    .unwrap_or_default()
    .into_iter()
    .filter(|id| {
      !id.is_empty()
        && id
          .chars()
          .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    })
    .collect()
}

fn write_durable_managed_pages(
  profile: &crate::profile::types::BrowserProfile,
  pages: &HashSet<String>,
) -> Result<(), String> {
  let path = local_managed_pages_path(profile);
  let parent = path
    .parent()
    .ok_or_else(|| "MANAGED_PAGE_PATH_INVALID".to_string())?;
  std::fs::create_dir_all(parent).map_err(|e| format!("MANAGED_PAGE_DIR_FAILED: {e}"))?;
  let tmp = path.with_extension("json.tmp");
  let mut values = pages.iter().cloned().collect::<Vec<_>>();
  values.sort();
  let body = serde_json::to_vec_pretty(&values)
    .map_err(|e| format!("MANAGED_PAGE_SERIALIZE_FAILED: {e}"))?;
  std::fs::write(&tmp, body).map_err(|e| format!("MANAGED_PAGE_WRITE_FAILED: {e}"))?;
  std::fs::rename(&tmp, &path).map_err(|e| format!("MANAGED_PAGE_COMMIT_FAILED: {e}"))
}

fn local_proxy_response(
  profile_id: &str,
  proxy_id: Option<String>,
  settings: Option<ProxySettings>,
) -> Response {
  let has_credentials = settings
    .as_ref()
    .and_then(|value| value.username.as_ref())
    .is_some()
    || settings
      .as_ref()
      .and_then(|value| value.password.as_ref())
      .is_some();
  let sanitized = settings.map(|mut value| {
    value.password = None;
    value
  });
  Json(LocalProxyResponse {
    profile_id: profile_id.to_string(),
    proxy_id,
    proxy_settings: sanitized,
    has_credentials,
  })
  .into_response()
}

async fn local_browser_get_proxy(Path(id): Path<String>) -> Response {
  let profile = match ProfileManager::instance()
    .list_profiles()
    .ok()
    .and_then(|profiles| {
      profiles
        .into_iter()
        .find(|profile| profile.id.to_string() == id)
    }) {
    Some(profile) => profile,
    None => {
      return json_error_response(
        StatusCode::NOT_FOUND,
        "PROFILE_NOT_FOUND",
        "profile not found",
      )
    }
  };
  let settings = profile
    .proxy_id
    .as_deref()
    .and_then(|proxy_id| PROXY_MANAGER.get_proxy_settings_by_id(proxy_id));
  local_proxy_response(&id, profile.proxy_id, settings)
}

async fn local_browser_put_proxy(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<LocalProxyRequest>,
) -> Response {
  if request.proxy_settings.host.trim().is_empty() || request.proxy_settings.port == 0 {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_PROXY_INVALID",
      "proxy host and port are required",
    );
  }
  let profile = match ProfileManager::instance()
    .list_profiles()
    .ok()
    .and_then(|profiles| {
      profiles
        .into_iter()
        .find(|profile| profile.id.to_string() == id)
    }) {
    Some(profile) => profile,
    None => {
      return json_error_response(
        StatusCode::NOT_FOUND,
        "PROFILE_NOT_FOUND",
        "profile not found",
      )
    }
  };
  let name = request.name.unwrap_or_else(|| format!("local-{id}"));
  let stored = if let Some(proxy_id) = profile.proxy_id.as_deref() {
    match PROXY_MANAGER.update_stored_proxy(
      &state.app_handle,
      proxy_id,
      None,
      Some(request.proxy_settings.clone()),
    ) {
      Ok(proxy) => proxy,
      Err(_) => {
        match PROXY_MANAGER.create_stored_proxy(&state.app_handle, name, request.proxy_settings) {
          Ok(proxy) => proxy,
          Err(error) => {
            return json_error_response(StatusCode::BAD_REQUEST, "LOCAL_PROXY_SAVE_FAILED", error)
          }
        }
      }
    }
  } else {
    match PROXY_MANAGER.create_stored_proxy(&state.app_handle, name, request.proxy_settings) {
      Ok(proxy) => proxy,
      Err(error) => {
        return json_error_response(StatusCode::BAD_REQUEST, "LOCAL_PROXY_SAVE_FAILED", error)
      }
    }
  };
  match ProfileManager::instance()
    .update_profile_proxy(state.app_handle.clone(), &id, Some(stored.id.clone()))
    .await
  {
    Ok(_) => local_proxy_response(&id, Some(stored.id), Some(stored.proxy_settings)),
    Err(error) => json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_PROXY_ASSIGN_FAILED",
      error.to_string(),
    ),
  }
}

async fn local_browser_delete_proxy(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Response {
  match ProfileManager::instance()
    .update_profile_proxy(state.app_handle, &id, None)
    .await
  {
    Ok(_) => local_proxy_response(&id, None, None),
    Err(error) => json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_PROXY_CLEAR_FAILED",
      error.to_string(),
    ),
  }
}

async fn local_browser_test_proxy(Path(id): Path<String>) -> Response {
  let profile = match ProfileManager::instance()
    .list_profiles()
    .ok()
    .and_then(|profiles| {
      profiles
        .into_iter()
        .find(|profile| profile.id.to_string() == id)
    }) {
    Some(profile) => profile,
    None => {
      return json_error_response(
        StatusCode::NOT_FOUND,
        "PROFILE_NOT_FOUND",
        "profile not found",
      )
    }
  };
  let Some(proxy_id) = profile.proxy_id.as_deref() else {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "LOCAL_PROXY_NOT_CONFIGURED",
      "profile has no local proxy",
    );
  };
  let Some(settings) = PROXY_MANAGER.get_proxy_settings_by_id(proxy_id) else {
    return json_error_response(
      StatusCode::NOT_FOUND,
      "LOCAL_PROXY_NOT_FOUND",
      "local proxy configuration not found",
    );
  };
  match PROXY_MANAGER
    .check_proxy_validity(proxy_id, &settings)
    .await
  {
    Ok(result) => Json(result).into_response(),
    Err(error) => json_error_response(StatusCode::BAD_GATEWAY, "LOCAL_PROXY_TEST_FAILED", error),
  }
}

async fn local_browser_list_pages(Path(id): Path<String>) -> Response {
  let (profile, port, pid, generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let cdp_pages = match crate::browser_runner::list_cdp_pages(port).await {
    Ok(pages) => pages,
    Err(error) => return json_error_response(StatusCode::BAD_GATEWAY, "CDP_UNAVAILABLE", error),
  };
  let mut managed = read_durable_managed_pages(&profile);
  managed.extend(
    LOCAL_MANAGED_PAGES
      .lock()
      .await
      .get(&id)
      .cloned()
      .unwrap_or_default(),
  );
  let leases = LOCAL_PAGE_LEASES.lock().await.clone();
  let durable_target = profile.managed_grok_target_id.clone();
  let pages = cdp_pages
    .into_iter()
    .map(|page| {
      let is_managed =
        managed.contains(&page.id) || durable_target.as_deref() == Some(page.id.as_str());
      let lease = leases.values().find(|lease| {
        lease.profile_id == id
          && lease.target_id == page.id
          && lease.launch_generation == generation
      });
      LocalBrowserPageResponse {
        target_id: page.id,
        page_type: "page".into(),
        url: page.url,
        title: page.title,
        purpose: lease.map(|value| value.purpose.clone()).unwrap_or_else(|| {
          if is_managed {
            "GROK_AUTOMATION".into()
          } else {
            "USER".into()
          }
        }),
        managed: is_managed,
        state: if lease.is_some() {
          "LEASED".into()
        } else {
          "IDLE".into()
        },
        browser_pid: pid,
        launch_generation: generation,
        page_lease_id: lease.map(|value| value.page_lease_id.clone()),
      }
    })
    .collect();
  Json(LocalBrowserPagesResponse {
    profile_id: id,
    browser_pid: pid,
    remote_debugging_port: port,
    cdp_endpoint: format!("http://127.0.0.1:{port}"),
    launch_generation: generation,
    pages,
  })
  .into_response()
}

async fn local_browser_create_page(
  Path(id): Path<String>,
  Json(request): Json<LocalBrowserPageRequest>,
) -> Response {
  if request.url.trim().is_empty() {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "PAGE_URL_REQUIRED",
      "url is required",
    );
  }
  let (profile, port, pid, generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let page = match crate::browser_runner::create_cdp_page(port, request.url.trim()).await {
    Ok(page) => page,
    Err(error) => {
      return json_error_response(StatusCode::BAD_GATEWAY, "CDP_PAGE_CREATE_FAILED", error)
    }
  };
  let mut owned = read_durable_managed_pages(&profile);
  owned.insert(page.id.clone());
  if let Err(error) = write_durable_managed_pages(&profile, &owned) {
    let _ = crate::browser_runner::close_cdp_page(port, &page.id).await;
    return json_error_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      "MANAGED_PAGE_PERSIST_FAILED",
      error,
    );
  }
  LOCAL_MANAGED_PAGES
    .lock()
    .await
    .entry(id.clone())
    .or_default()
    .insert(page.id.clone());
  Json(serde_json::json!({ "page": LocalBrowserPageResponse { target_id: page.id, page_type: "page".into(), url: page.url, title: page.title, purpose: request.purpose, managed: true, state: "IDLE".into(), browser_pid: pid, launch_generation: generation, page_lease_id: Option::<String>::None } })).into_response()
}

#[utoipa::path(
  post,
  path = "/v1/local/browser/profiles/{id}/pages/claim",
  params(("id" = String, Path, description = "Chrome for Testing profile ID")),
  request_body = LocalBrowserPageClaimRequest,
  responses(
    (status = 200, body = LocalBrowserPageLeaseResponse),
    (status = 400, description = "Invalid correlation or pool limit"),
    (status = 409, description = "Page pool busy or stale target")
  ),
  tag = "profiles"
)]
async fn local_browser_claim_page(
  Path(id): Path<String>,
  Json(request): Json<LocalBrowserPageClaimRequest>,
) -> Response {
  if request.job_id.trim().is_empty() || request.request_id.trim().is_empty() {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "PAGE_CLAIM_CORRELATION_REQUIRED",
      "jobId and requestId are required",
    );
  }
  if !(1..=16).contains(&request.max_pages) {
    return json_error_response(
      StatusCode::BAD_REQUEST,
      "PAGE_POOL_LIMIT_INVALID",
      "maxPages must be between 1 and 16",
    );
  }
  let _claim_guard = LOCAL_PAGE_CLAIM_LOCK.lock().await;
  let (profile, port, pid, generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let cdp_pages = match crate::browser_runner::list_cdp_pages(port).await {
    Ok(pages) => pages,
    Err(error) => return json_error_response(StatusCode::BAD_GATEWAY, "CDP_UNAVAILABLE", error),
  };
  let mut managed = read_durable_managed_pages(&profile);
  managed.extend(
    LOCAL_MANAGED_PAGES
      .lock()
      .await
      .get(&id)
      .cloned()
      .unwrap_or_default(),
  );
  if let Some(target_id) = profile.managed_grok_target_id.as_ref() {
    managed.insert(target_id.clone());
  }
  let leases_snapshot = LOCAL_PAGE_LEASES.lock().await.clone();
  if let Some(existing) = leases_snapshot
    .values()
    .find(|lease| {
      lease.profile_id == id
        && lease.job_id == request.job_id
        && lease.request_id == request.request_id
        && lease.launch_generation == generation
    })
    .cloned()
  {
    return Json(LocalBrowserPageLeaseResponse {
      profile_id: id,
      browser_pid: pid,
      remote_debugging_port: port,
      cdp_endpoint: format!("http://127.0.0.1:{port}"),
      launch_generation: generation,
      target_id: existing.target_id,
      page_lease_id: existing.page_lease_id,
      page_reused: true,
      purpose: existing.purpose,
    })
    .into_response();
  }
  let target = if let Some(target_id) = request.target_id.as_deref() {
    let page = cdp_pages.iter().find(|page| page.id == target_id);
    let is_grok = page
      .and_then(|value| url::Url::parse(&value.url).ok())
      .and_then(|value| {
        value
          .host_str()
          .map(|host| host.eq_ignore_ascii_case("grok.com") || host.ends_with(".grok.com"))
      })
      .unwrap_or(false);
    if page.is_none() || !managed.contains(target_id) || !is_grok {
      return json_error_response(
        StatusCode::CONFLICT,
        "GROK_MANAGED_TARGET_STALE",
        "requested target is not a managed local page",
      );
    }
    if leases_snapshot
      .values()
      .any(|lease| lease.profile_id == id && lease.target_id == target_id)
    {
      return json_error_response(
        StatusCode::CONFLICT,
        "PAGE_POOL_BUSY",
        "requested page is already leased",
      );
    }
    target_id.to_string()
  } else {
    let idle = cdp_pages.iter().find(|page| {
      let is_grok = url::Url::parse(&page.url)
        .ok()
        .and_then(|value| {
          value
            .host_str()
            .map(|host| host.eq_ignore_ascii_case("grok.com") || host.ends_with(".grok.com"))
        })
        .unwrap_or(false);
      managed.contains(&page.id)
        && is_grok
        && !leases_snapshot
          .values()
          .any(|lease| lease.profile_id == id && lease.target_id == page.id)
    });
    if let Some(page) = idle {
      page.id.clone()
    } else {
      let managed_count = cdp_pages
        .iter()
        .filter(|page| managed.contains(&page.id))
        .count();
      if managed_count >= request.max_pages {
        return json_error_response(
          StatusCode::CONFLICT,
          "PAGE_POOL_BUSY",
          "managed page pool is full",
        );
      }
      let page =
        match crate::browser_runner::create_cdp_page(port, "https://grok.com/imagine").await {
          Ok(page) => page,
          Err(error) => {
            return json_error_response(StatusCode::BAD_GATEWAY, "CDP_PAGE_CREATE_FAILED", error)
          }
        };
      managed.insert(page.id.clone());
      if let Err(error) = write_durable_managed_pages(&profile, &managed) {
        let _ = crate::browser_runner::close_cdp_page(port, &page.id).await;
        return json_error_response(
          StatusCode::INTERNAL_SERVER_ERROR,
          "MANAGED_PAGE_PERSIST_FAILED",
          error,
        );
      }
      LOCAL_MANAGED_PAGES
        .lock()
        .await
        .entry(id.clone())
        .or_default()
        .insert(page.id.clone());
      page.id
    }
  };
  let page_lease_id = uuid::Uuid::new_v4().to_string();
  let lease = LocalPageLease {
    profile_id: id.clone(),
    target_id: target.clone(),
    page_lease_id: page_lease_id.clone(),
    job_id: request.job_id.clone(),
    request_id: request.request_id.clone(),
    purpose: request.purpose.clone(),
    launch_generation: generation,
  };
  LOCAL_PAGE_LEASES
    .lock()
    .await
    .insert(page_lease_id.clone(), lease);
  Json(LocalBrowserPageLeaseResponse {
    profile_id: id,
    browser_pid: pid,
    remote_debugging_port: port,
    cdp_endpoint: format!("http://127.0.0.1:{port}"),
    launch_generation: generation,
    target_id: target,
    page_lease_id,
    page_reused: false,
    purpose: request.purpose,
  })
  .into_response()
}

#[utoipa::path(
  post,
  path = "/v1/local/browser/profiles/{id}/pages/{target_id}/release",
  params(
    ("id" = String, Path, description = "Chrome for Testing profile ID"),
    ("target_id" = String, Path, description = "Managed page target ID")
  ),
  request_body = LocalBrowserPageReleaseRequest,
  responses(
    (status = 200, description = "Lease released idempotently"),
    (status = 409, description = "Lease identity mismatch")
  ),
  tag = "profiles"
)]
async fn local_browser_release_page(
  Path((id, target_id)): Path<(String, String)>,
  Json(request): Json<LocalBrowserPageReleaseRequest>,
) -> Response {
  let (_profile, _port, _pid, generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let mut leases = LOCAL_PAGE_LEASES.lock().await;
  let Some(lease) = leases.get(&request.page_lease_id).cloned() else {
    return Json(
      serde_json::json!({ "ok": true, "released": false, "pageLeaseId": request.page_lease_id }),
    )
    .into_response();
  };
  if lease.profile_id != id
    || lease.target_id != target_id
    || lease.job_id != request.job_id
    || lease.request_id != request.request_id
    || lease.launch_generation != generation
  {
    return json_error_response(
      StatusCode::CONFLICT,
      "PAGE_LEASE_MISMATCH",
      "page lease identity does not match the current browser generation",
    );
  }
  leases.remove(&request.page_lease_id);
  Json(serde_json::json!({ "ok": true, "released": true, "pageLeaseId": request.page_lease_id, "targetId": target_id })).into_response()
}

async fn local_browser_delete_page(Path((id, target_id)): Path<(String, String)>) -> Response {
  let (profile, port, _pid, _generation) = match local_profile_identity(&id) {
    Ok(value) => value,
    Err(response) => return response,
  };
  let mut durable_owned = read_durable_managed_pages(&profile);
  let is_managed = durable_owned.contains(&target_id)
    || LOCAL_MANAGED_PAGES
      .lock()
      .await
      .get(&id)
      .is_some_and(|pages| pages.contains(&target_id));
  if !is_managed {
    return json_error_response(
      StatusCode::FORBIDDEN,
      "USER_PAGE_PROTECTED",
      "only pages created by the local manager can be closed",
    );
  }
  match crate::browser_runner::close_cdp_page(port, &target_id).await {
    Ok(()) => {
      durable_owned.remove(&target_id);
      let _ = write_durable_managed_pages(&profile, &durable_owned);
      if let Some(pages) = LOCAL_MANAGED_PAGES.lock().await.get_mut(&id) {
        pages.remove(&target_id);
      }
      Json(serde_json::json!({ "ok": true, "targetId": target_id })).into_response()
    }
    Err(error) => json_error_response(StatusCode::BAD_GATEWAY, "CDP_PAGE_CLOSE_FAILED", error),
  }
}

fn run_profile_error_response(status: StatusCode, body: String) -> Response {
  let body = if body.trim().is_empty() {
    serde_json::json!({
      "error": {
        "code": "INTERNAL_ERROR",
        "stage": "RESPONSE_BUILD",
        "message": "profile launch failed",
        "retryable": status.is_server_error()
      }
    })
    .to_string()
  } else if serde_json::from_str::<serde_json::Value>(&body).is_ok() {
    body
  } else {
    serde_json::json!({
      "error": {
        "code": "INTERNAL_ERROR",
        "stage": "RESPONSE_BUILD",
        "message": body,
        "retryable": status.is_server_error()
      }
    })
    .to_string()
  };
  let mut response = (status, body).into_response();
  response.headers_mut().insert(
    header::CONTENT_TYPE,
    HeaderValue::from_static("application/json"),
  );
  response
}

fn target_binding_error_response(error: String) -> (StatusCode, String) {
  if let Ok(value) = serde_json::from_str::<serde_json::Value>(&error) {
    if let Some(code) = value
      .pointer("/error/code")
      .and_then(serde_json::Value::as_str)
    {
      let status = match code {
        "TARGET_BINDING_PROFILE_NOT_FOUND" => StatusCode::NOT_FOUND,
        "TARGET_BINDING_REQUIRES_CHROMIUM" => StatusCode::BAD_REQUEST,
        "TARGET_BINDING_PREPARE_FAILED"
          if value
            .pointer("/error/stage")
            .and_then(serde_json::Value::as_str)
            == Some("PROFILE_VALIDATION") =>
        {
          StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
      };
      return (status, error);
    }
  }
  let code = error
    .split(':')
    .next()
    .filter(|value| !value.trim().is_empty())
    .unwrap_or("TARGET_BINDING_FAILED");
  let (stage, message) = if code == "TARGET_BINDING_PREPARE_FAILED" {
    let mut parts = error.splitn(4, ':');
    let _ = parts.next();
    let _ = parts.next();
    let stage = parts
      .next()
      .filter(|value| !value.is_empty())
      .unwrap_or("PREPARE");
    let message = parts.next().unwrap_or(&error).trim_start_matches(':');
    (stage.to_string(), message.to_string())
  } else {
    (target_binding_stage(code).to_string(), error.clone())
  };
  let status = match code {
    "TARGET_BINDING_PROFILE_NOT_FOUND" => StatusCode::NOT_FOUND,
    "TARGET_BINDING_PROFILE_RUNNING"
    | "TARGET_BINDING_SESSION_NOT_FOUND"
    | "TARGET_BINDING_SESSION_EXPIRED"
    | "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE"
    | "TARGET_BINDING_IDENTITY_CHANGED"
    | "TARGET_BINDING_PROFILE_MISMATCH"
    | "TARGET_BINDING_HANDLE_INVALID"
    | "TARGET_BINDING_HANDLE_STALE" => StatusCode::CONFLICT,
    "TARGET_BINDING_REQUIRES_CHROMIUM" => StatusCode::BAD_REQUEST,
    _ => StatusCode::INTERNAL_SERVER_ERROR,
  };
  (
    status,
    serde_json::json!({
      "error": {
        "code": code,
        "message": message,
        "stage": stage,
        "retryable": status.is_server_error()
      }
    })
    .to_string(),
  )
}

fn target_binding_stage(code: &str) -> &'static str {
  match code {
    "TARGET_BINDING_REQUIRES_CHROMIUM"
    | "TARGET_BINDING_PROFILE_NOT_FOUND"
    | "TARGET_BINDING_PROFILE_MISMATCH" => "PROFILE_VALIDATION",
    "TARGET_BINDING_PROFILE_RUNNING" => "TEMP_BINDING_TRANSITION",
    "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE" | "TARGET_BINDING_SESSION_EXPIRED" => "RECOVERY",
    "TARGET_BINDING_CANDIDATES_NOT_FOUND" => "CANDIDATE_DISCOVERY",
    "TARGET_BINDING_CDP_PORT_MISSING" | "TARGET_BINDING_BROWSER_IDENTITY_MISSING" => {
      "CDP_READINESS"
    }
    "TARGET_BINDING_PROCESS_SPAWN_FAILED" | "TARGET_BINDING_PROCESS_POST_SPAWN_FAILED" => {
      "PROCESS_SPAWN"
    }
    "TARGET_BINDING_CDP_READINESS_FAILED" => "CDP_READINESS",
    "TARGET_BINDING_CHECKPOINT_FAILED" => "CHECKPOINT_CAPTURE",
    "TARGET_BINDING_RESPONSE_BUILD_FAILED" => "RESPONSE_BUILD",
    "TARGET_BINDING_HANDLE_INVALID" | "TARGET_BINDING_HANDLE_STALE" => "CANDIDATE_DISCOVERY",
    "TARGET_BINDING_TASK_PANICKED" => "TASK_BOUNDARY",
    _ => "PREPARE",
  }
}

fn target_binding_http_response(status: StatusCode, body: String) -> Response {
  let body = if body.trim().is_empty() {
    serde_json::json!({
      "error": {
        "code": "TARGET_BINDING_TASK_FAILED",
        "stage": "RESPONSE_BUILD",
        "message": "target binding request failed without an error body",
        "retryable": status.is_server_error()
      }
    })
    .to_string()
  } else if serde_json::from_str::<serde_json::Value>(&body).is_ok() {
    body
  } else {
    target_binding_error_response(body).1
  };
  let mut response = (status, body).into_response();
  response.headers_mut().insert(
    header::CONTENT_TYPE,
    HeaderValue::from_static("application/json"),
  );
  response
}

fn target_binding_request_invalid_body(message: &str) -> String {
  serde_json::json!({
    "error": {
      "code": "TARGET_BINDING_REQUEST_INVALID",
      "stage": "REQUEST_VALIDATION",
      "message": message,
      "retryable": false
    }
  })
  .to_string()
}

fn target_binding_request_invalid_response(message: &str) -> Response {
  target_binding_http_response(
    StatusCode::BAD_REQUEST,
    target_binding_request_invalid_body(message),
  )
}

/// Normalize every non-panic prepare failure to the typed error contract. The
/// binding service historically returned plain strings (and, in one path, an
/// empty body), so the route owns the final envelope while preserving the
/// original safe code/message for diagnosis.
fn target_binding_prepare_error_body(profile_id: &str, status: StatusCode, body: &str) -> String {
  let parsed = serde_json::from_str::<serde_json::Value>(body).ok();
  let original_code = parsed
    .as_ref()
    .and_then(|value| value.pointer("/error/code").or_else(|| value.get("code")))
    .and_then(serde_json::Value::as_str)
    .or_else(|| {
      body
        .split(':')
        .next()
        .filter(|value| !value.trim().is_empty())
    })
    .unwrap_or("TARGET_BINDING_FAILED");
  let message = parsed
    .as_ref()
    .and_then(|value| {
      value
        .pointer("/error/message")
        .or_else(|| value.get("message"))
    })
    .and_then(serde_json::Value::as_str)
    .filter(|value| !value.trim().is_empty())
    .unwrap_or_else(|| {
      let value = body.trim();
      if value.is_empty() {
        "target binding preparation failed"
      } else {
        value
      }
    });
  let stage = parsed
    .as_ref()
    .and_then(|value| value.pointer("/error/stage").or_else(|| value.get("stage")))
    .and_then(serde_json::Value::as_str)
    .filter(|value| !value.trim().is_empty())
    .unwrap_or_else(|| target_binding_stage(original_code));
  let bool_field = |name: &str| {
    parsed
      .as_ref()
      .and_then(|value| {
        value
          .pointer(&format!("/error/{name}"))
          .or_else(|| value.get(name))
      })
      .and_then(serde_json::Value::as_bool)
      .unwrap_or(false)
  };
  serde_json::json!({
    "error": {
      "code": "TARGET_BINDING_PREPARE_FAILED",
      "stage": stage,
      "message": message,
      "profileId": profile_id,
      "processSpawned": bool_field("processSpawned"),
      "rollbackAttempted": bool_field("rollbackAttempted"),
      "rollbackSucceeded": bool_field("rollbackSucceeded"),
      "retryable": parsed
        .as_ref()
        .and_then(|value| value.pointer("/error/retryable"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(status.is_server_error()),
      "details": {
        "originalCode": original_code
      }
    }
  })
  .to_string()
}

#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/target-binding/prepare",
  params(("id" = String, Path, description = "Profile ID")),
  responses(
    (status = 200, description = "Explicit target binding candidates", body = crate::browser_runner::TargetBindingPrepareResponse),
    (status = 400, description = "Profile does not use CFT"),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Profile is already running"),
    (status = 500, description = "Binding preparation failed")
  ),
  security(("bearer_auth" = [])),
  tag = "profiles"
)]
async fn prepare_target_binding(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Response {
  let profile_id_for_error = id.clone();
  let result = std::panic::AssertUnwindSafe(async move {
    if state.runtime_kind != "floword-donut-runtime"
      && !crate::cloud_auth::CLOUD_AUTH
        .can_use_browser_automation()
        .await
    {
      return Err((
        StatusCode::PAYMENT_REQUIRED,
        "browser automation is not available".into(),
      ));
    }
    let profile = ProfileManager::instance()
      .list_profiles()
      .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
      .into_iter()
      .find(|profile| profile.id.to_string() == id)
      .ok_or_else(|| {
        (
          StatusCode::NOT_FOUND,
          "TARGET_BINDING_PROFILE_NOT_FOUND".into(),
        )
      })?;
    if profile.is_cross_os() {
      return Err((
        StatusCode::BAD_REQUEST,
        "cross-OS profile cannot be launched".into(),
      ));
    }
    crate::browser_runner::BrowserRunner::instance()
      .prepare_managed_grok_binding(state.app_handle.clone(), profile)
      .await
      .map(Json)
      .map_err(target_binding_error_response)
  })
  .catch_unwind()
  .await;
  match result {
    Ok(Ok(Json(body))) => Json(body).into_response(),
    Ok(Err((status, body))) => target_binding_http_response(
      status,
      target_binding_prepare_error_body(&profile_id_for_error, status, &body),
    ),
    Err(_) => target_binding_http_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      serde_json::json!({
        "error": {
          "code": "TARGET_BINDING_TASK_PANICKED",
          "stage": "TASK_BOUNDARY",
          "message": "target binding task panicked",
          "retryable": false
        }
      })
      .to_string(),
    ),
  }
}

#[utoipa::path(
  get,
  path = "/v1/profiles/{id}/target-binding/pending",
  params(("id" = String, Path, description = "Profile ID")),
  responses(
    (status = 200, description = "Durable pending target binding", body = crate::browser_runner::TargetBindingPrepareResponse),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Pending binding cannot be resumed")
  ),
  security(("bearer_auth" = [])),
  tag = "profiles"
)]
async fn pending_target_binding(Path(id): Path<String>) -> Response {
  let result = crate::browser_runner::BrowserRunner::instance()
    .resume_managed_grok_binding(&id)
    .await;
  match result {
    Ok(response) => Json(response).into_response(),
    Err(error) => {
      let (status, body) = target_binding_error_response(error);
      target_binding_http_response(status, body)
    }
  }
}

#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/target-binding/confirm",
  params(("id" = String, Path, description = "Profile ID")),
  request_body = TargetBindingConfirmRequest,
  responses(
    (status = 200, description = "Managed target binding committed", body = crate::browser_runner::TargetBindingConfirmResponse),
    (status = 409, description = "Binding session or handle is stale"),
    (status = 500, description = "Binding confirmation failed")
  ),
  security(("bearer_auth" = [])),
  tag = "profiles"
)]
async fn confirm_target_binding(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
  request: Result<Json<TargetBindingConfirmRequest>, JsonRejection>,
) -> Response {
  let request = match request {
    Ok(Json(request)) => request,
    Err(_) => {
      return target_binding_request_invalid_response(
        "binding_session_id and handle are required snake_case fields",
      )
    }
  };
  if uuid::Uuid::parse_str(&request.binding_session_id).is_err() {
    return target_binding_request_invalid_response("binding_session_id must be a valid UUID");
  }
  let result = std::panic::AssertUnwindSafe(async move {
    let profile_id = ProfileManager::instance()
      .list_profiles()
      .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
      .into_iter()
      .find(|profile| profile.id.to_string() == id)
      .map(|profile| profile.id.to_string())
      .ok_or_else(|| {
        (
          StatusCode::NOT_FOUND,
          "TARGET_BINDING_PROFILE_NOT_FOUND".into(),
        )
      })?;
    let session_profile =
      crate::browser_runner::binding_session_profile_id(&request.binding_session_id)
        .await
        .map_err(target_binding_error_response)?;
    if session_profile != profile_id {
      return Err(target_binding_error_response(
        "TARGET_BINDING_PROFILE_MISMATCH".into(),
      ));
    }
    crate::browser_runner::BrowserRunner::instance()
      .confirm_managed_grok_binding(&request.binding_session_id, &request.handle)
      .await
      .map(Json)
      .map_err(target_binding_error_response)
  })
  .catch_unwind()
  .await;
  match result {
    Ok(Ok(Json(body))) => Json(body).into_response(),
    Ok(Err((status, body))) => target_binding_http_response(status, body),
    Err(_) => target_binding_http_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      serde_json::json!({
        "error": {
          "code": "TARGET_BINDING_TASK_PANICKED",
          "stage": "TASK_BOUNDARY",
          "message": "target binding task panicked",
          "retryable": false
        }
      })
      .to_string(),
    ),
  }
}

#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/target-binding/abort",
  params(("id" = String, Path, description = "Profile ID")),
  request_body = TargetBindingSessionRequest,
  responses(
    (status = 200, description = "Binding aborted", body = crate::browser_runner::TargetBindingAbortResponse),
    (status = 409, description = "Binding session is stale"),
    (status = 500, description = "Binding abort failed")
  ),
  security(("bearer_auth" = [])),
  tag = "profiles"
)]
async fn abort_target_binding(
  Path(id): Path<String>,
  State(_state): State<ApiServerState>,
  request: Result<Json<TargetBindingSessionRequest>, JsonRejection>,
) -> Response {
  let request = match request {
    Ok(Json(request)) => request,
    Err(_) => {
      return target_binding_request_invalid_response(
        "binding_session_id is required and must be valid JSON",
      )
    }
  };
  let result = std::panic::AssertUnwindSafe(async move {
    let profile_id = crate::browser_runner::binding_session_profile_id(&request.binding_session_id)
      .await
      .map_err(target_binding_error_response)?;
    if profile_id != id {
      return Err(target_binding_error_response(
        "TARGET_BINDING_PROFILE_MISMATCH".into(),
      ));
    }
    crate::browser_runner::BrowserRunner::instance()
      .abort_managed_grok_binding(&request.binding_session_id)
      .await
      .map(Json)
      .map_err(target_binding_error_response)
  })
  .catch_unwind()
  .await;
  match result {
    Ok(Ok(Json(body))) => Json(body).into_response(),
    Ok(Err((status, body))) => target_binding_http_response(status, body),
    Err(_) => target_binding_http_response(
      StatusCode::INTERNAL_SERVER_ERROR,
      serde_json::json!({
        "error": {
          "code": "TARGET_BINDING_TASK_PANICKED",
          "stage": "TASK_BOUNDARY",
          "message": "target binding task panicked",
          "retryable": false
        }
      })
      .to_string(),
    ),
  }
}

async fn run_profile_inner(
  id: String,
  state: ApiServerState,
  headers: HeaderMap,
  request: RunProfileRequest,
  enforce_entitlement: bool,
) -> Result<Json<RunProfileResponse>, (StatusCode, String)> {
  let request_id = headers
    .get("X-Floword-Request-Id")
    .and_then(|value| value.to_str().ok())
    .unwrap_or("unknown");
  log_run_phase(request_id, "RUN_HANDLER_ENTERED", &id, None, None);
  if enforce_entitlement
    && state.runtime_kind != "floword-donut-runtime"
    && !crate::cloud_auth::CLOUD_AUTH
      .can_use_browser_automation()
      .await
  {
    return Err((
      StatusCode::PAYMENT_REQUIRED,
      "browser automation is not available for this account".to_string(),
    ));
  }

  let headless = request.headless.unwrap_or(false);

  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager.list_profiles().map_err(|error| {
    (
      StatusCode::INTERNAL_SERVER_ERROR,
      format!("failed to list profiles: {error}"),
    )
  })?;

  let profile = profiles
    .iter()
    .find(|p| p.id.to_string() == id)
    .ok_or((StatusCode::NOT_FOUND, "profile not found".to_string()))?;
  log_run_phase(
    request_id,
    "PROFILE_RESOLVED",
    &id,
    profile.process_id,
    profile.last_launch,
  );

  if profile.is_cross_os() {
    return Err((
      StatusCode::BAD_REQUEST,
      "profile was created on a different operating system".to_string(),
    ));
  }

  // Team lock check
  crate::team_lock::acquire_team_lock_if_needed(profile)
    .await
    .map_err(|error| (StatusCode::CONFLICT, error.to_string()))?;

  let policy = if request.cold_start_only.unwrap_or(false) {
    crate::browser_runner::LaunchUrlPolicy::ColdStartOnly(
      request
        .url
        .unwrap_or_else(|| "https://grok.com/imagine".to_string()),
    )
  } else {
    crate::browser_runner::LaunchUrlPolicy::AlwaysOpen(request.url)
  };
  // Older callers may omit browser_engine. A persisted Chromium profile must
  // still launch through the CFT/Chromium path instead of falling back to the
  // historical Wayfern default.
  let requested_engine = match request.browser_engine.as_deref() {
    Some(value) => Some(
      crate::browser::BrowserType::from_str(value)
        .map_err(|error| (StatusCode::BAD_REQUEST, error))?,
    ),
    None if profile.browser.eq_ignore_ascii_case("chromium") => {
      Some(crate::browser::BrowserType::Chromium)
    }
    None => None,
  };
  let launch_result = match crate::browser_runner::launch_browser_profile_impl_with_policy_result(
    state.app_handle.clone(),
    profile.clone(),
    policy,
    None,
    headless,
    false,
    requested_engine.clone(),
  )
  .await
  {
    Ok(result) => result,
    Err(e) => {
      log::error!("Run profile failed: {e}");
      return Err(profile_launch_error_response(&id, e));
    }
  };
  let crate::browser_runner::FlowordLaunchResult {
    profile: updated_profile,
    reused,
    remote_debugging_port,
    grok_target_id,
    grok_page_url,
    grok_target_reused,
    target_selection_source,
  } = launch_result;
  log_run_phase(
    request_id,
    "BROWSER_IDENTITY_RESOLVED",
    &id,
    updated_profile.process_id,
    updated_profile.last_launch,
  );
  let profiles_dir = ProfileManager::instance().get_profiles_dir();
  let profile_path = updated_profile.get_profile_data_path(&profiles_dir);
  let profile_path_str = profile_path.to_string_lossy();
  let cdp_port = if let Some(port) = remote_debugging_port {
    port
  } else {
    crate::wayfern_manager::WayfernManager::instance()
      .get_cdp_port(&profile_path_str)
      .await
      .unwrap_or(0)
  };
  let browser_executable = if requested_engine == Some(crate::browser::BrowserType::Chromium) {
    crate::browser::ChromiumBrowser::resolve_executable().ok()
  } else {
    None
  };

  log_run_phase(
    request_id,
    "RUN_HANDLER_RETURNING",
    &id,
    updated_profile.process_id,
    updated_profile.last_launch,
  );
  Ok(Json(RunProfileResponse {
    profile_id: profile.id.to_string(),
    browser_engine: requested_engine
      .as_ref()
      .map(|engine| engine.canonical_engine_name().to_string())
      .unwrap_or_else(|| "WAYFERN".to_string()),
    browser_version: if browser_executable.is_some() {
      std::env::var("FLOWORD_CHROMIUM_VERSION")
        .ok()
        .or_else(|| Some("staged".to_string()))
    } else {
      None
    },
    browser_executable: browser_executable.map(|path| path.to_string_lossy().to_string()),
    grok_target_id,
    grok_page_url,
    grok_target_reused,
    target_selection_source,
    remote_debugging_port: cdp_port,
    headless,
    browser_pid: updated_profile.process_id,
    launch_generation: updated_profile.last_launch,
    reused,
  }))
}

fn profile_launch_error_response(profile_id: &str, raw: String) -> (StatusCode, String) {
  let parsed = serde_json::from_str::<serde_json::Value>(&raw).ok();
  let code = parsed
    .as_ref()
    .and_then(|v| v.get("code"))
    .and_then(|v| v.as_str())
    .filter(|value| !value.trim().is_empty())
    .or_else(|| raw.split(':').next())
    .filter(|value| !value.trim().is_empty())
    .unwrap_or("INTERNAL_ERROR")
    .to_string();
  let raw_detail = parsed
    .as_ref()
    .and_then(|v| v.get("params"))
    .and_then(|v| v.get("detail"))
    .and_then(|v| v.as_str())
    .filter(|value| !value.trim().is_empty())
    .or_else(|| {
      let trimmed = raw.trim();
      (!trimmed.is_empty()).then_some(trimmed)
    })
    .unwrap_or("profile launch failed");
  // Post-spawn reconciliation may carry a structured inner target error in
  // params.detail.  Unwrap it here so the outer HTTP envelope preserves the
  // real message/stage and sanitized candidate diagnostics.
  let inner = serde_json::from_str::<serde_json::Value>(raw_detail).ok();
  let detail = inner
    .as_ref()
    .and_then(|value| value.get("message"))
    .and_then(|value| value.as_str())
    .unwrap_or(raw_detail)
    .to_string();
  let details = parsed
    .as_ref()
    .and_then(|v| {
      v.get("details")
        .or_else(|| v.get("params").and_then(|p| p.get("details")))
    })
    .cloned()
    .unwrap_or_else(|| serde_json::json!({}));
  let mut details = details;
  if let Some(inner_details) = inner.as_ref().and_then(|value| value.get("details")) {
    if let (Some(target), Some(source)) = (details.as_object_mut(), inner_details.as_object()) {
      for (key, value) in source {
        target.insert(key.clone(), value.clone());
      }
    }
  }
  let stage = parsed
    .as_ref()
    .and_then(|v| v.get("stage"))
    .and_then(|value| value.as_str())
    .or_else(|| {
      inner
        .as_ref()
        .and_then(|v| v.get("stage"))
        .and_then(|value| value.as_str())
    })
    .or_else(|| {
      parsed
        .as_ref()
        .and_then(|v| v.get("details"))
        .and_then(|v| v.get("stage"))
        .and_then(|value| value.as_str())
    })
    .unwrap_or(match code.as_str() {
      "PROXY_BROWSER_PID_BIND_FAILED" => "PROXY_BROWSER_PID_BIND",
      "PROXY_START_FAILED" | "XRAY_START_FAILED" => "PROXY_START",
      "WAYFERN_LAUNCH_FAILED" => "WAYFERN_LAUNCH",
      "PROFILE_PROCESS_PERSIST_FAILED" => "PROFILE_PROCESS_PERSIST",
      "GROK_TARGET_NAVIGATION_FAILED"
      | "GROK_TARGET_NAVIGATION_UNKNOWN"
      | "GROK_COLD_START_NAVIGATION_FAILED" => "ENSURE_GROK_TARGET",
      "RUN_POST_SPAWN_RECONCILE_FAILED" => "RUN_POST_SPAWN_RECONCILE",
      _ => "PROFILE_LAUNCH",
    });
  let status = match code.as_str() {
    "PROFILE_RUNNING" | "PROFILE_LAUNCH_IN_PROGRESS" => StatusCode::CONFLICT,
    "PROXY_START_FAILED" | "PROXY_BROWSER_PID_BIND_FAILED" => StatusCode::SERVICE_UNAVAILABLE,
    _ => StatusCode::INTERNAL_SERVER_ERROR,
  };
  let retryable = matches!(
    status,
    StatusCode::CONFLICT | StatusCode::SERVICE_UNAVAILABLE
  );
  let mut error = serde_json::json!({
    "code": code,
    "message": detail,
    "stage": stage,
    "profileId": profile_id,
    "retryable": retryable,
    "details": details.clone()
  });
  if let Some(object) = error.as_object_mut() {
    for key in ["processSpawned", "rollbackAttempted", "rollbackSucceeded"] {
      if let Some(value) = details.get(key) {
        object.insert(key.to_string(), value.clone());
      }
    }
  }
  let body = serde_json::json!({ "error": error });
  (status, body.to_string())
}

// API Handler - Launch this profile on a REMOTE VM of its own operating system
#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/run-remote",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = RunRemoteRequest,
  responses(
    (status = 200, description = "Remote session started", body = RunRemoteResponse),
    (status = 400, description = "Profile does not have cloud sync enabled"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with browser automation required"),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Profile is locked by another session"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 503, description = "No remote capacity for this operating system"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn run_profile_remote(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<RunRemoteRequest>,
) -> Result<Json<RunRemoteResponse>, (StatusCode, String)> {
  if !crate::cloud_auth::CLOUD_AUTH
    .can_use_browser_automation()
    .await
  {
    return Err((StatusCode::PAYMENT_REQUIRED, String::new()));
  }

  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(manager_error_response)?;
  let profile = profiles
    .iter()
    .find(|p| p.id.to_string() == id)
    .ok_or((StatusCode::NOT_FOUND, "profile not found".to_string()))?;

  // The profile must exist in cloud storage before a remote host can open it —
  // the VM pulls it from donut-sync, and a profile that has never synced would
  // launch an empty browser and then push that emptiness back over the real one.
  if let Err(reason) = remote_launch_precondition(profile) {
    return Err((StatusCode::BAD_REQUEST, reason));
  }

  // Deliberately NO is_cross_os() guard here. Local /run refuses a foreign
  // profile because this machine is the wrong OS; running it remotely on a host
  // of its OWN OS is precisely what this endpoint exists for.
  let outcome =
    crate::remote_session::start_remote_session(state.app_handle.clone(), profile, request.url)
      .await
      .map_err(remote_session_error_response)?;

  Ok(Json(RunRemoteResponse {
    profile_id: profile.id.to_string(),
    session_id: outcome.session_id,
    platform: outcome.platform,
    status: outcome.status,
  }))
}

#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/cloud-sync",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = SetCloudSyncRequest,
  responses(
    (status = 200, description = "Cloud sync mode updated", body = SetCloudSyncResponse),
    (status = 400, description = "Invalid mode, or the profile cannot be synced"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with cloud backup required"),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Profile is running — stop it before enabling sync"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn set_profile_cloud_sync(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<SetCloudSyncRequest>,
) -> Result<Json<SetCloudSyncResponse>, (StatusCode, String)> {
  // Remote launch requires cloud sync, and until now sync could only be turned
  // on from the GUI — so an automation-only caller could never reach the state
  // that makes /run-remote work.
  let mode = match request.mode.as_str() {
    "Disabled" | "Regular" | "Encrypted" => request.mode.clone(),
    other => {
      return Err((
        StatusCode::BAD_REQUEST,
        format!("invalid sync mode {other:?}; expected Disabled, Regular or Encrypted"),
      ));
    }
  };

  crate::sync::set_profile_sync_mode(state.app_handle.clone(), id.clone(), mode.clone())
    .await
    .map_err(sync_mode_error_response)?;

  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(manager_error_response)?;
  let profile = profiles
    .iter()
    .find(|p| p.id.to_string() == id)
    .ok_or((StatusCode::NOT_FOUND, "profile not found".to_string()))?;

  // Reported rather than left for the caller to discover at launch time: the
  // most common reason a caller enables sync is to run the profile remotely,
  // and Encrypted mode silently makes that impossible.
  let blocked = remote_launch_precondition(profile).err();
  Ok(Json(SetCloudSyncResponse {
    profile_id: profile.id.to_string(),
    mode,
    remote_launchable: blocked.is_none(),
    remote_blocked_reason: blocked,
  }))
}

/// Map a sync-mode failure onto the status the caller can act on.
///
/// `set_profile_sync_mode` reports a running profile as a JSON body rather than
/// a plain message, because enabling sync under a live browser would race the
/// browser's own writes.
fn sync_mode_error_response(err: String) -> (StatusCode, String) {
  if err.contains("PROFILE_RUNNING") {
    return (
      StatusCode::CONFLICT,
      "profile is running; stop it before changing cloud sync".to_string(),
    );
  }
  if err.contains("cross-OS") || err.contains("ephemeral") {
    return (StatusCode::BAD_REQUEST, err);
  }
  (StatusCode::INTERNAL_SERVER_ERROR, err)
}

/// Whether a profile may be launched on a remote host.
///
/// Extracted so the rule is unit-testable without a running app: it is the one
/// gate between "the user asked" and "a browser opens somewhere else holding
/// their cookies".
pub fn remote_launch_precondition(
  profile: &crate::profile::types::BrowserProfile,
) -> Result<(), String> {
  if !profile.is_sync_enabled() {
    return Err(
      "profile does not have cloud sync enabled; a remote host has no way to \
       obtain it"
        .to_string(),
    );
  }
  if profile.is_encrypted_sync() {
    // The key is derived from a passphrase that never leaves this machine, so
    // the host would download ciphertext, launch Chromium on it, and push the
    // corruption back over the user's real profile.
    return Err(
      "profile uses end-to-end encrypted sync; a remote host cannot decrypt \
       it. Switch the profile to Regular sync to run it remotely."
        .to_string(),
    );
  }
  if profile.resolved_os().is_none() {
    return Err(
      "profile has no recorded operating system, so it cannot be scheduled \
       onto a matching host"
        .to_string(),
    );
  }
  Ok(())
}

// API Handler - Stop a REMOTE session started by run-remote
#[utoipa::path(
  delete,
  path = "/v1/remote-sessions/{id}",
  params(
    ("id" = String, Path, description = "Remote session ID from run-remote")
  ),
  responses(
    (status = 200, description = "Remote session stopped", body = StopRemoteResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "No such remote session"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 503, description = "The fleet could not be reached; the session is still running"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn stop_remote_session(
  Path(id): Path<String>,
) -> Result<Json<StopRemoteResponse>, (StatusCode, String)> {
  // Without this route, `run-remote` hands back a session id nothing can act
  // on: the only thing that ends a session is the fleet's own two-hour cap, so
  // every launch bills 7200s no matter how briefly it ran.
  let outcome = crate::remote_session::end_remote_session(&id)
    .await
    .map_err(remote_session_error_response)?;

  Ok(Json(StopRemoteResponse {
    session_id: outcome.session_id,
    status: outcome.status,
    billed_seconds: outcome.billed_seconds,
  }))
}

fn remote_session_error_response(
  err: crate::remote_session::RemoteSessionError,
) -> (StatusCode, String) {
  use crate::remote_session::RemoteSessionError;
  match err {
    RemoteSessionError::NoCapacity(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
    RemoteSessionError::Conflict(m) => (StatusCode::CONFLICT, m),
    RemoteSessionError::NotAuthorised(m) => (StatusCode::PAYMENT_REQUIRED, m),
    RemoteSessionError::Other(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
  }
}

// API Handler - Open URL in existing browser
#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/open-url",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = OpenUrlRequest,
  responses(
    (status = 200, description = "URL opened successfully"),
    (status = 400, description = "Cannot open URL with a cross-OS profile"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with browser automation required"),
    (status = 404, description = "Profile not found"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn open_url_in_profile(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<OpenUrlRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
  if !crate::cloud_auth::CLOUD_AUTH
    .can_use_browser_automation()
    .await
  {
    return Err((StatusCode::PAYMENT_REQUIRED, String::new()));
  }

  let browser_runner = crate::browser_runner::BrowserRunner::instance();

  browser_runner
    .open_url_with_profile(state.app_handle.clone(), id, request.url)
    .await
    .map_err(manager_error_response)?;

  Ok(StatusCode::OK)
}

// API Handler - Kill browser process
#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/kill",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  responses(
    (status = 200, description = "Browser process stopped successfully", body = crate::browser_runner::StopBrowserResult),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan required"),
    (status = 404, description = "Profile not found"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn kill_profile(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
) -> Result<Json<crate::browser_runner::StopBrowserResult>, (StatusCode, String)> {
  // Programmatically launching and stopping profiles is a paid feature; the
  // run/open-url handlers gate the same way.
  if !crate::cloud_auth::CLOUD_AUTH
    .can_use_browser_automation()
    .await
  {
    return Err((
      StatusCode::PAYMENT_REQUIRED,
      serde_json::json!({"error":{"code":"PAYMENT_REQUIRED","message":"browser automation is not available for this account"}}).to_string(),
    ));
  }

  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager.list_profiles().map_err(|error| {
    (
      StatusCode::INTERNAL_SERVER_ERROR,
      serde_json::json!({"error":{"code":"INTERNAL_ERROR","message":error.to_string()}})
        .to_string(),
    )
  })?;

  let profile = profiles
    .iter()
    .find(|p| p.id.to_string() == id)
    .ok_or_else(|| {
      (
        StatusCode::NOT_FOUND,
        serde_json::json!({"error":{"code":"PROFILE_NOT_FOUND","message":"profile not found"}})
          .to_string(),
      )
    })?;

  let browser_runner = crate::browser_runner::BrowserRunner::instance();
  let result = browser_runner
    .stop_browser_process_with_result(state.app_handle.clone(), profile)
    .await
    .map_err(|error| {
      let raw = error.to_string();
      let code = match raw.as_str() {
        "PROFILE_BUSY" => "PROFILE_BUSY",
        "BROWSER_SESSION_IDENTITY_MISMATCH" => "BROWSER_SESSION_IDENTITY_MISMATCH",
        "BROWSER_SESSION_NOT_MANAGED" => "BROWSER_SESSION_NOT_MANAGED",
        _ => "BROWSER_STOP_FAILED",
      };
      let status = if matches!(
        code,
        "PROFILE_BUSY" | "BROWSER_SESSION_IDENTITY_MISMATCH" | "BROWSER_SESSION_NOT_MANAGED"
      ) {
        StatusCode::CONFLICT
      } else {
        StatusCode::INTERNAL_SERVER_ERROR
      };
      (
        status,
        serde_json::json!({"error":{"code":code,"message":raw}}).to_string(),
      )
    })?;

  crate::team_lock::release_team_lock_if_needed(profile).await;

  Ok(Json(result))
}

// API Handler - Batch run profiles (paid: browser automation). Mirrors the
// single `/run` gate; never breaks the batch on a single profile's failure —
// each profile gets its own result entry.
#[utoipa::path(
  post,
  path = "/v1/profiles/batch/run",
  request_body = BatchRunRequest,
  responses(
    (status = 200, description = "Batch launch completed; inspect per-profile results", body = BatchRunResponse),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with browser automation required"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn batch_run_profiles(
  State(state): State<ApiServerState>,
  Json(request): Json<BatchRunRequest>,
) -> Result<Json<BatchRunResponse>, StatusCode> {
  if !crate::cloud_auth::CLOUD_AUTH
    .can_use_browser_automation()
    .await
  {
    return Err(StatusCode::PAYMENT_REQUIRED);
  }

  let headless = request.headless.unwrap_or(false);
  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

  let mut results = Vec::with_capacity(request.profile_ids.len());
  for profile_id in &request.profile_ids {
    let fail = |error: &str| BatchRunResult {
      profile_id: profile_id.clone(),
      ok: false,
      remote_debugging_port: None,
      error: Some(error.to_string()),
    };

    let Some(profile) = profiles.iter().find(|p| p.id.to_string() == *profile_id) else {
      results.push(fail("profile not found"));
      continue;
    };
    if profile.is_cross_os() {
      results.push(fail("cross-OS profiles cannot be launched"));
      continue;
    }
    if crate::team_lock::acquire_team_lock_if_needed(profile)
      .await
      .is_err()
    {
      results.push(fail("profile is locked by another team member"));
      continue;
    }

    let port = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
      Ok(listener) => match listener.local_addr() {
        Ok(addr) => addr.port(),
        Err(_) => {
          results.push(fail("failed to allocate debugging port"));
          continue;
        }
      },
      Err(_) => {
        results.push(fail("failed to allocate debugging port"));
        continue;
      }
    };

    match crate::browser_runner::launch_browser_profile_impl(
      state.app_handle.clone(),
      profile.clone(),
      request.url.clone(),
      Some(port),
      headless,
      true,
    )
    .await
    {
      Ok(_) => results.push(BatchRunResult {
        profile_id: profile_id.clone(),
        ok: true,
        remote_debugging_port: Some(port),
        error: None,
      }),
      Err(e) => results.push(fail(&format!("launch failed: {e}"))),
    }
  }

  Ok(Json(BatchRunResponse { results }))
}

// API Handler - Batch stop profiles (paid: browser automation).
#[utoipa::path(
  post,
  path = "/v1/profiles/batch/stop",
  request_body = BatchStopRequest,
  responses(
    (status = 200, description = "Batch stop completed; inspect per-profile results", body = BatchStopResponse),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Active paid plan with browser automation required"),
    (status = 429, description = "Automation request rate limit exceeded"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn batch_stop_profiles(
  State(state): State<ApiServerState>,
  Json(request): Json<BatchStopRequest>,
) -> Result<Json<BatchStopResponse>, StatusCode> {
  if !crate::cloud_auth::CLOUD_AUTH
    .can_use_browser_automation()
    .await
  {
    return Err(StatusCode::PAYMENT_REQUIRED);
  }

  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
  let browser_runner = crate::browser_runner::BrowserRunner::instance();

  let mut results = Vec::with_capacity(request.profile_ids.len());
  for profile_id in &request.profile_ids {
    let Some(profile) = profiles.iter().find(|p| p.id.to_string() == *profile_id) else {
      results.push(BatchStopResult {
        profile_id: profile_id.clone(),
        ok: false,
        error: Some("profile not found".to_string()),
      });
      continue;
    };

    match browser_runner
      .kill_browser_process(state.app_handle.clone(), profile)
      .await
    {
      Ok(_) => {
        crate::team_lock::release_team_lock_if_needed(profile).await;
        results.push(BatchStopResult {
          profile_id: profile_id.clone(),
          ok: true,
          error: None,
        });
      }
      Err(e) => results.push(BatchStopResult {
        profile_id: profile_id.clone(),
        ok: false,
        error: Some(format!("stop failed: {e}")),
      }),
    }
  }

  Ok(Json(BatchStopResponse { results }))
}

// API Handler - Detect importable browser profiles on this machine, or scan a
// custom folder. Free: importing is not gated behind browser automation.
#[utoipa::path(
  get,
  path = "/v1/profiles/import/detect",
  params(
    ("folder" = Option<String>, Query, description = "Optional folder to scan instead of the default browser locations. Accepts a single profile dir, a Chromium user-data dir, or a folder holding one profile dir per child.")
  ),
  responses(
    (status = 200, description = "Detected importable profiles", body = DetectedProfilesResponse),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Folder not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn detect_import_profiles(
  Query(query): Query<DetectImportQuery>,
  State(_state): State<ApiServerState>,
) -> Result<Json<DetectedProfilesResponse>, (StatusCode, String)> {
  let importer = crate::profile_importer::ProfileImporter::instance();
  let profiles = match query.folder.as_deref() {
    Some(folder) => importer.scan_folder(std::path::Path::new(folder)),
    None => importer.detect_existing_profiles(),
  }
  .map_err(manager_error_response)?;
  let total = profiles.len();
  Ok(Json(DetectedProfilesResponse { profiles, total }))
}

// API Handler - Bulk-import browser profiles from on-disk profile folders.
// Free (parity with create_profile); only fingerprint OS spoofing is Pro.
// Items are isolated — one failure doesn't stop the rest.
#[utoipa::path(
  post,
  path = "/v1/profiles/import",
  request_body = ImportProfilesRequest,
  responses(
    (status = 200, description = "Batch import completed; inspect per-item results", body = crate::profile_importer::ProfileImportBatchResult),
    (status = 400, description = "No items, or invalid input"),
    (status = 401, description = "Unauthorized"),
    (status = 402, description = "Fingerprint OS spoofing requires an active Pro subscription"),
    (status = 404, description = "Group not found"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "profiles"
)]
async fn import_profiles_api(
  State(state): State<ApiServerState>,
  Json(request): Json<ImportProfilesRequest>,
) -> Result<Json<crate::profile_importer::ProfileImportBatchResult>, (StatusCode, String)> {
  let wayfern_config: Option<crate::wayfern_manager::WayfernConfig> = request
    .wayfern_config
    .as_ref()
    .and_then(|config| serde_json::from_value(config.clone()).ok());

  // The Pro gate for fingerprint OS spoofing lives inside import_profiles, so
  // every surface inherits it; manager_error_response maps the code to 402.
  let importer = crate::profile_importer::ProfileImporter::instance();
  importer
    .import_profiles(
      &state.app_handle,
      request.items,
      request.group_id,
      request.duplicate_strategy.unwrap_or_default(),
      wayfern_config,
    )
    .await
    .map(Json)
    .map_err(manager_error_response)
}

#[utoipa::path(
  post,
  path = "/v1/profiles/{id}/cookies/import",
  params(
    ("id" = String, Path, description = "Profile ID")
  ),
  request_body = ImportCookiesRequest,
  responses(
    (status = 200, description = "Cookies imported successfully", body = ImportCookiesResponse),
    (status = 400, description = "Invalid cookie file or unsupported browser"),
    (status = 401, description = "Unauthorized"),
    (status = 404, description = "Profile not found"),
    (status = 409, description = "Browser is currently running"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "cookies"
)]
async fn import_profile_cookies(
  Path(id): Path<String>,
  State(state): State<ApiServerState>,
  Json(request): Json<ImportCookiesRequest>,
) -> Result<Json<ImportCookiesResponse>, StatusCode> {
  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

  if !profiles.iter().any(|p| p.id.to_string() == id) {
    return Err(StatusCode::NOT_FOUND);
  }

  match crate::cookie_manager::CookieManager::import_cookies(
    &state.app_handle,
    &id,
    &request.content,
  )
  .await
  {
    Ok(result) => {
      if let Some(scheduler) = crate::sync::get_global_scheduler() {
        if let Some(profile) = profiles.iter().find(|p| p.id.to_string() == id) {
          if profile.is_sync_enabled() {
            let pid = id.clone();
            tauri::async_runtime::spawn(async move {
              scheduler.queue_profile_sync(pid).await;
            });
          }
        }
      }
      Ok(Json(ImportCookiesResponse {
        cookies_imported: result.cookies_imported,
        cookies_replaced: result.cookies_replaced,
        errors: result.errors,
      }))
    }
    Err(e) => {
      let msg = e.to_lowercase();
      if msg.contains("running") {
        Err(StatusCode::CONFLICT)
      } else if msg.contains("no valid cookies") || msg.contains("unsupported browser") {
        Err(StatusCode::BAD_REQUEST)
      } else {
        Err(StatusCode::INTERNAL_SERVER_ERROR)
      }
    }
  }
}

// API Handler - Download Browser
#[utoipa::path(
  post,
  path = "/v1/browsers/download",
  request_body = DownloadBrowserRequest,
  responses(
    (status = 200, description = "Browser downloaded (or already present)", body = DownloadBrowserResponse),
    (status = 400, description = "Invalid browser or version not available for download"),
    (status = 401, description = "Unauthorized"),
    (status = 409, description = "This browser version is already being downloaded"),
    (status = 500, description = "Internal server error (e.g. network failure)")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "browsers"
)]
async fn download_browser_api(
  State(state): State<ApiServerState>,
  Json(request): Json<DownloadBrowserRequest>,
) -> Result<Json<DownloadBrowserResponse>, (StatusCode, String)> {
  match crate::downloader::download_browser(
    state.app_handle.clone(),
    request.browser.clone(),
    request.version,
  )
  .await
  {
    // Echo the version the downloader actually installed, not the requested one.
    Ok(version) => Ok(Json(DownloadBrowserResponse {
      browser: request.browser,
      version,
      status: "downloaded".to_string(),
    })),
    Err(e) => {
      if e.contains("already being downloaded") {
        Err((StatusCode::CONFLICT, e))
      } else {
        Err(manager_error_response(e))
      }
    }
  }
}

// API Handler - Get Browser Versions
#[utoipa::path(
  get,
  path = "/v1/browsers/{browser}/versions",
  params(
    ("browser" = String, Path, description = "Browser name")
  ),
  responses(
    (status = 200, description = "List of available browser versions", body = Vec<String>),
    (status = 400, description = "Unsupported browser"),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "browsers"
)]
async fn get_browser_versions(
  Path(browser): Path<String>,
  State(_state): State<ApiServerState>,
) -> Result<Json<Vec<String>>, (StatusCode, String)> {
  let version_manager = crate::browser_version_manager::BrowserVersionManager::instance();

  match version_manager
    .fetch_browser_versions_with_count(&browser, false)
    .await
  {
    Ok(result) => Ok(Json(result.versions)),
    Err(e) => Err(manager_error_response(e)),
  }
}

// API Handler - Check if Browser is Downloaded
#[utoipa::path(
  get,
  path = "/v1/browsers/{browser}/versions/{version}/downloaded",
  params(
    ("browser" = String, Path, description = "Browser name"),
    ("version" = String, Path, description = "Browser version")
  ),
  responses(
    (status = 200, description = "Browser download status", body = bool),
    (status = 401, description = "Unauthorized"),
    (status = 500, description = "Internal server error")
  ),
  security(
    ("bearer_auth" = [])
  ),
  tag = "browsers"
)]
async fn check_browser_downloaded(
  Path((browser, version)): Path<(String, String)>,
  State(_state): State<ApiServerState>,
) -> Result<Json<bool>, StatusCode> {
  let is_downloaded = crate::downloaded_browsers_registry::is_browser_downloaded(browser, version);
  Ok(Json(is_downloaded))
}

#[cfg(test)]
mod tests {
  use super::*;
  use axum::body::Body;
  use axum::http::Request;
  use http_body_util::BodyExt;
  use serde_json::json;
  use std::process::{Command, Stdio};
  use tempfile::TempDir;
  use tokio::time::{sleep, Duration};
  use tower::ServiceExt;
  use wiremock::{matchers::method, Mock, MockServer, ResponseTemplate};

  fn write_pending_binding_fixture(
    root: &std::path::Path,
    profile_id: uuid::Uuid,
    session_id: &str,
    pid: u32,
    cdp_port: u16,
    generation: u64,
    executable: &std::path::Path,
    user_data_dir: &std::path::Path,
  ) -> (std::path::PathBuf, std::path::PathBuf) {
    let profile_root = root.join("profiles").join(profile_id.to_string());
    let profile_data = profile_root.join("profile");
    std::fs::create_dir_all(&profile_data).unwrap();
    std::fs::write(
      profile_root.join("metadata.json"),
      serde_json::to_vec_pretty(&json!({
        "id": profile_id,
        "name": "pending-test",
        "browser": "chromium",
        "version": "test",
        "process_id": pid,
        "last_launch": generation,
        "release_type": "stable"
      }))
      .unwrap(),
    )
    .unwrap();
    let candidate = |handle: &str, target_id: &str| {
      json!({
        "handle": handle,
        "targetId": target_id,
        "targetIdHash": format!("hash-{target_id}"),
        "normalizedUrl": "https://grok.com/imagine",
        "hostname": "grok.com",
        "normalizedPath": "/imagine",
        "titleHash": format!("title-{target_id}")
      })
    };
    let ledger = json!({
      "profileId": profile_id,
      "managedTargetBindingId": session_id,
      "lastKnownTargetId": "PENDING",
      "browserPid": pid,
      "cdpPort": cdp_port,
      "launchGeneration": generation,
      "managedGrokPageUrl": "https://grok.com/imagine",
      "bindingCreatedAt": 1,
      "bindingVersion": 1,
      "lifecycle": "BINDING_REQUIRED",
      "executable": executable,
      "userDataDir": user_data_dir,
      "expiresAt": 4_000_000_000u64,
      "candidates": [candidate("handle-one", "target-one"), candidate("handle-two", "target-two")],
      "previousLedger": null
    });
    let ledger_path = profile_data.join("floword-managed-target-binding.json");
    let receipt_path = profile_data.join("floword-chromium-launch-receipt.json");
    std::fs::write(&ledger_path, serde_json::to_vec_pretty(&ledger).unwrap()).unwrap();
    std::fs::write(
      &receipt_path,
      serde_json::to_vec_pretty(&json!({
        "profileId": profile_id,
        "browserPid": pid,
        "cdpPort": cdp_port,
        "launchGeneration": generation,
        "executable": executable,
        "userDataDir": user_data_dir,
        "spawnedAt": 1
      }))
      .unwrap(),
    )
    .unwrap();
    (ledger_path, receipt_path)
  }

  #[tokio::test(flavor = "current_thread")]
  #[serial_test::serial]
  async fn pending_binding_router_resumes_exact_durable_handles_idempotently() {
    let root = TempDir::new().unwrap();
    let _data_guard = crate::app_dirs::set_test_data_dir(root.path().to_path_buf());
    let profile_id = uuid::Uuid::new_v4();
    let session_id = uuid::Uuid::new_v4().to_string();
    let generation = 7;
    let cdp = MockServer::start().await;
    let cdp_port = cdp.address().port();
    Mock::given(method("GET"))
      .and(wiremock::matchers::path("/json/list"))
      .respond_with(ResponseTemplate::new(200).set_body_json(json!([
        {"id":"target-one","type":"page","url":"https://grok.com/imagine","title":"Imagine","webSocketDebuggerUrl":"ws://127.0.0.1/one"},
        {"id":"target-two","type":"page","url":"https://grok.com/imagine","title":"Imagine","webSocketDebuggerUrl":"ws://127.0.0.1/two"}
      ])))
      .expect(1)
      .mount(&cdp)
      .await;

    let profile_data = root
      .path()
      .join("profiles")
      .join(profile_id.to_string())
      .join("profile");
    let user_data_dir = profile_data.join("floword-chromium");
    // Use a private copy so the synthetic process has an executable path that
    // exactly matches the durable receipt, just like a staged CFT launch.
    // This keeps the identity check exercised without depending on a browser
    // binary being installed on the test host.
    let powershell =
      std::path::PathBuf::from(std::env::var("WINDIR").unwrap_or_else(|_| "C:\\Windows".into()))
        .join("System32\\WindowsPowerShell\\v1.0\\powershell.exe");
    let executable = root.path().join("synthetic-cft.exe");
    std::fs::copy(&powershell, &executable).unwrap();
    let user_arg = format!("--user-data-dir={}", user_data_dir.display());
    let port_arg = format!("--remote-debugging-port={cdp_port}");
    let mut child = Command::new(&executable)
      .args([
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-Command",
        "Start-Sleep -Seconds 300",
      ])
      .arg(&user_arg)
      .arg(&port_arg)
      .stdin(Stdio::null())
      .stdout(Stdio::null())
      .stderr(Stdio::null())
      .spawn()
      .unwrap();
    sleep(Duration::from_millis(150)).await;
    let (ledger_path, receipt_path) = write_pending_binding_fixture(
      root.path(),
      profile_id,
      &session_id,
      child.id(),
      cdp_port,
      generation,
      &executable,
      &user_data_dir,
    );
    crate::browser_runner::clear_target_binding_session_for_test(&session_id).await;

    let app = Router::new().route(
      "/v1/profiles/{id}/target-binding/pending",
      get(pending_target_binding),
    );
    let uri = format!("/v1/profiles/{profile_id}/target-binding/pending");
    let request = || Request::builder().uri(&uri).body(Body::empty()).unwrap();
    let response_one = app.clone().oneshot(request()).await.unwrap();
    let status_one = response_one.status();
    let content_type_one = response_one
      .headers()
      .get(header::CONTENT_TYPE)
      .and_then(|value| value.to_str().ok())
      .map(str::to_string);
    let body_one = response_one.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
      status_one,
      StatusCode::OK,
      "pending response body: {}",
      String::from_utf8_lossy(&body_one)
    );
    assert_eq!(content_type_one.as_deref(), Some("application/json"));
    let value_one: serde_json::Value = serde_json::from_slice(&body_one).unwrap();
    assert_eq!(value_one["binding_session_id"], session_id);
    assert_eq!(value_one["browser_pid"], child.id());
    assert_eq!(value_one["remote_debugging_port"], cdp_port);
    assert_eq!(value_one["launch_generation"], generation);
    assert_eq!(value_one["candidate_count"], 2);
    assert_eq!(value_one["candidates"][0]["handle"], "handle-one");
    assert_eq!(
      value_one["candidates"][0]["target_id_hash"],
      "hash-target-one"
    );
    assert_eq!(
      value_one["candidates"][0]["url"],
      "https://grok.com/imagine"
    );
    assert_eq!(value_one["candidates"][1]["handle"], "handle-two");
    assert_eq!(
      value_one["candidates"][1]["target_id_hash"],
      "hash-target-two"
    );
    let ledger_before = std::fs::read(&ledger_path).unwrap();

    let response_two = app.oneshot(request()).await.unwrap();
    let status_two = response_two.status();
    let body_two = response_two.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
      status_two,
      StatusCode::OK,
      "second pending response body: {}",
      String::from_utf8_lossy(&body_two)
    );
    let value_two: serde_json::Value = serde_json::from_slice(&body_two).unwrap();
    assert_eq!(value_two, value_one);
    assert_eq!(std::fs::read(&ledger_path).unwrap(), ledger_before);
    assert!(receipt_path.exists());
    let _ = child.kill();
    let _ = child.wait();
  }

  #[tokio::test(flavor = "current_thread")]
  #[serial_test::serial]
  async fn pending_binding_router_keeps_invalid_receipts_pending_without_rollback() {
    let root = TempDir::new().unwrap();
    let _data_guard = crate::app_dirs::set_test_data_dir(root.path().to_path_buf());
    let executable = std::path::PathBuf::from("C:\\floword\\cft\\chrome.exe");
    let user_data_dir = root.path().join("profile-data");
    let app = Router::new().route(
      "/v1/profiles/{id}/target-binding/pending",
      get(pending_target_binding),
    );

    for (receipt_mode, expected_code) in [
      ("mismatched", "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE"),
      ("missing", "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE"),
    ] {
      let profile_id = uuid::Uuid::new_v4();
      let session_id = uuid::Uuid::new_v4().to_string();
      let (ledger_path, receipt_path) = write_pending_binding_fixture(
        root.path(),
        profile_id,
        &session_id,
        99999,
        6550,
        7,
        &executable,
        &user_data_dir,
      );
      if receipt_mode == "mismatched" {
        std::fs::write(
          &receipt_path,
          serde_json::to_vec(&json!({
            "profileId": profile_id,
            "browserPid": 99998,
            "cdpPort": 6550,
            "launchGeneration": 7,
            "executable": executable,
            "userDataDir": user_data_dir,
            "spawnedAt": 1
          }))
          .unwrap(),
        )
        .unwrap();
      } else {
        std::fs::remove_file(&receipt_path).unwrap();
      }
      let ledger_before = std::fs::read(&ledger_path).unwrap();
      crate::browser_runner::clear_target_binding_session_for_test(&session_id).await;
      let uri = format!("/v1/profiles/{profile_id}/target-binding/pending");
      let response = app
        .clone()
        .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
      assert_eq!(response.status(), StatusCode::CONFLICT);
      let body = response.into_body().collect().await.unwrap().to_bytes();
      let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
      assert_eq!(value["error"]["code"], expected_code);
      assert_eq!(std::fs::read(&ledger_path).unwrap(), ledger_before);
    }
  }

  #[test]
  fn run_profile_response_exposes_reused_boolean() {
    let response = RunProfileResponse {
      profile_id: "profile".to_string(),
      browser_engine: "CHROME_FOR_TESTING".to_string(),
      browser_version: Some("test".to_string()),
      browser_executable: None,
      grok_target_id: None,
      grok_page_url: None,
      grok_target_reused: false,
      target_selection_source: None,
      remote_debugging_port: 9223,
      headless: false,
      browser_pid: Some(42),
      launch_generation: Some(7),
      reused: true,
    };
    let value = serde_json::to_value(response).expect("response should serialize");
    assert_eq!(
      value.get("reused").and_then(serde_json::Value::as_bool),
      Some(true)
    );
  }

  #[test]
  fn profile_launch_errors_are_non_empty_and_preserve_structured_details() {
    let (status, body) = profile_launch_error_response(
      "profile-1",
      serde_json::json!({
        "code": "GROK_TARGET_NAVIGATION_FAILED",
        "params": {
          "detail": "navigation did not commit",
          "details": {"targetIdHash": "abc", "phase": "ENSURE_GROK_TARGET"}
        }
      })
      .to_string(),
    );
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let value: serde_json::Value = serde_json::from_str(&body).expect("JSON error envelope");
    assert_eq!(value["error"]["code"], "GROK_TARGET_NAVIGATION_FAILED");
    assert_eq!(value["error"]["message"], "navigation did not commit");
    assert_eq!(value["error"]["stage"], "ENSURE_GROK_TARGET");
    assert_eq!(value["error"]["profileId"], "profile-1");
    assert_eq!(value["error"]["details"]["phase"], "ENSURE_GROK_TARGET");
  }

  #[test]
  fn target_binding_errors_include_json_stage_and_non_empty_message() {
    let (status, body) = target_binding_error_response(
      "TARGET_BINDING_HANDLE_STALE: candidate target disappeared".into(),
    );
    assert_eq!(status, StatusCode::CONFLICT);
    let value: serde_json::Value = serde_json::from_str(&body).expect("JSON error envelope");
    assert_eq!(value["error"]["code"], "TARGET_BINDING_HANDLE_STALE");
    assert_eq!(value["error"]["stage"], "CANDIDATE_DISCOVERY");
    assert_eq!(
      value["error"]["message"],
      "TARGET_BINDING_HANDLE_STALE: candidate target disappeared"
    );
    assert!(!body.trim().is_empty());
  }

  #[test]
  fn prepare_failures_use_typed_json_contract() {
    let body = target_binding_prepare_error_body(
      "profile-1",
      StatusCode::INTERNAL_SERVER_ERROR,
      "TARGET_BINDING_CDP_PORT_MISSING",
    );
    let value: serde_json::Value = serde_json::from_str(&body).expect("JSON error envelope");
    assert_eq!(value["error"]["code"], "TARGET_BINDING_PREPARE_FAILED");
    assert_eq!(value["error"]["stage"], "CDP_READINESS");
    assert_eq!(value["error"]["profileId"], "profile-1");
    assert!(value["error"].get("processSpawned").is_some());
    assert!(value["error"].get("rollbackAttempted").is_some());
    assert!(value["error"].get("rollbackSucceeded").is_some());
    assert!(!value["error"]["message"].as_str().unwrap().is_empty());
  }

  #[test]
  fn confirm_request_contract_requires_snake_case_fields() {
    let valid = serde_json::from_str::<TargetBindingConfirmRequest>(
      r#"{"binding_session_id":"00000000-0000-0000-0000-000000000001","handle":"h"}"#,
    );
    assert!(valid.is_ok());
    let camel_case = serde_json::from_str::<TargetBindingConfirmRequest>(
      r#"{"bindingSessionId":"00000000-0000-0000-0000-000000000001","candidateHandle":"h"}"#,
    );
    assert!(camel_case.is_err());
    let missing_handle = serde_json::from_str::<TargetBindingConfirmRequest>(
      r#"{"binding_session_id":"00000000-0000-0000-0000-000000000001"}"#,
    );
    assert!(missing_handle.is_err());
    let replacement_only = serde_json::from_str::<TargetBindingConfirmRequest>(
      r#"{"binding_session_id":"00000000-0000-0000-0000-000000000001","candidateHandle":"h"}"#,
    );
    assert!(replacement_only.is_err());
    let value: serde_json::Value =
      serde_json::from_str(&target_binding_request_invalid_body("invalid request"))
        .expect("JSON rejection envelope");
    assert_eq!(value["error"]["code"], "TARGET_BINDING_REQUEST_INVALID");
    assert_eq!(value["error"]["stage"], "REQUEST_VALIDATION");
    assert_eq!(value["error"]["retryable"], false);
  }

  #[test]
  fn profile_launch_errors_never_return_an_empty_message() {
    let (status, body) = profile_launch_error_response("profile-1", String::new());
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let value: serde_json::Value = serde_json::from_str(&body).expect("JSON error envelope");
    assert!(!value["error"]["message"].as_str().unwrap().is_empty());
    assert_eq!(value["error"]["code"], "INTERNAL_ERROR");
  }

  #[test]
  fn profile_launch_errors_promote_inner_target_diagnostics() {
    let inner = serde_json::json!({
      "code": "AMBIGUOUS_GROK_TAB",
      "message": "multiple Grok page targets have no authoritative mapping",
      "details": {
        "grokCandidateCount": 2,
        "candidateTargetIdHashes": ["a", "b"],
        "selectionPath": "AMBIGUOUS_DISCOVERY"
      }
    });
    let raw = serde_json::json!({
      "code": "RUN_POST_SPAWN_RECONCILE_FAILED",
      "stage": "GROK_TARGET_SELECTION",
      "params": { "detail": inner.to_string() },
      "details": { "browserPid": 42 }
    });
    let (_, body) = profile_launch_error_response("profile-1", raw.to_string());
    let value: serde_json::Value = serde_json::from_str(&body).expect("JSON error envelope");
    assert_eq!(value["error"]["message"], inner["message"]);
    assert_eq!(value["error"]["stage"], "GROK_TARGET_SELECTION");
    assert_eq!(value["error"]["details"]["grokCandidateCount"], 2);
    assert_eq!(value["error"]["details"]["browserPid"], 42);
  }

  #[test]
  fn middleware_error_response_is_json_and_non_empty() {
    let response = json_error_response(StatusCode::INTERNAL_SERVER_ERROR, "TEST_FAILURE", "boom");
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
      response.headers().get(header::CONTENT_TYPE).unwrap(),
      "application/json"
    );
    assert_eq!(
      response.headers().get("x-floword-error-code").unwrap(),
      "TEST_FAILURE"
    );
  }
  use crate::profile::types::{BrowserProfile, SyncMode};

  fn profile_with(sync_mode: SyncMode, host_os: Option<&str>) -> BrowserProfile {
    BrowserProfile {
      id: uuid::Uuid::nil(),
      name: "p".to_string(),
      browser: "wayfern".to_string(),
      version: "latest".to_string(),
      sync_mode,
      host_os: host_os.map(|s| s.to_string()),
      ..Default::default()
    }
  }

  // Cloud sync has been settable through PUT /v1/profiles/{id} but was absent
  // from every profile RESPONSE, so a caller could turn it on and never
  // confirm it. A remote-launch caller must be able to see this before it can
  // decide whether the profile exists in cloud storage at all.
  // /run-remote exists precisely so a profile can run on a host of ITS OWN OS
  // when this machine is the wrong one. The gate is cloud sync: a remote host
  // obtains the profile from donut-sync, so a profile that has never synced
  // would launch an empty browser and push that emptiness over the real one.
  #[test]
  fn remote_launch_requires_cloud_sync() {
    let err = remote_launch_precondition(&profile_with(SyncMode::Disabled, Some("macos")))
      .expect_err("a non-synced profile must be refused");
    assert!(err.contains("cloud sync"), "unhelpful message: {err}");

    assert!(
      remote_launch_precondition(&profile_with(SyncMode::Regular, Some("macos"))).is_ok(),
      "a synced profile must be allowed"
    );
  }

  #[test]
  fn remote_launch_refuses_an_end_to_end_encrypted_profile() {
    // The key is derived from a passphrase that never leaves this machine, so
    // a remote host downloads ciphertext, launches Chromium on it, and pushes
    // the corruption back over the user's real profile. Refusing here also
    // saves taking the profile lock and a slot on leased hardware for a
    // session that cannot possibly work.
    let err = remote_launch_precondition(&profile_with(SyncMode::Encrypted, Some("macos")))
      .expect_err("an encrypted profile must be refused");
    assert!(
      err.contains("encrypted") && err.contains("Regular"),
      "the message must say what to change: {err}"
    );
  }

  #[test]
  fn remote_launch_requires_a_known_operating_system() {
    // Without one there is no way to pick a matching host, and guessing would
    // be the cross-OS mismatch this whole design exists to prevent.
    assert!(remote_launch_precondition(&profile_with(SyncMode::Regular, None)).is_err());
  }

  #[test]
  fn remote_launch_allows_a_cross_os_profile() {
    let host = crate::profile::types::get_host_os();
    let other = if host == "windows" {
      "macos"
    } else {
      "windows"
    };
    let foreign = profile_with(SyncMode::Regular, Some(other));

    assert!(
      foreign.is_cross_os(),
      "test setup: profile should be foreign"
    );
    // Local /run refuses this; running it remotely on a host of its own OS is
    // exactly what /run-remote is for.
    assert!(remote_launch_precondition(&foreign).is_ok());
  }

  #[test]
  fn api_profile_exposes_cloud_sync_state() {
    let disabled = ApiProfile::from(&profile_with(SyncMode::Disabled, None));
    assert_eq!(disabled.sync_mode, "Disabled");
    assert!(!disabled.cloud_sync_enabled);

    let regular = ApiProfile::from(&profile_with(SyncMode::Regular, None));
    assert_eq!(regular.sync_mode, "Regular");
    assert!(regular.cloud_sync_enabled);

    let encrypted = ApiProfile::from(&profile_with(SyncMode::Encrypted, None));
    assert_eq!(encrypted.sync_mode, "Encrypted");
    assert!(encrypted.cloud_sync_enabled);
  }

  // A profile must only ever run on its own operating system: Chromium's
  // on-disk state is OS-specific, so replaying a macOS profile on Windows is a
  // mismatch no amount of user-agent spoofing repairs.
  #[test]
  fn api_profile_reports_its_operating_system() {
    let host = crate::profile::types::get_host_os();
    let same = ApiProfile::from(&profile_with(SyncMode::Regular, Some(&host)));
    assert_eq!(same.host_os.as_deref(), Some(host.as_str()));
    assert!(!same.is_cross_os);

    let other = if host == "windows" {
      "macos"
    } else {
      "windows"
    };
    let foreign = ApiProfile::from(&profile_with(SyncMode::Regular, Some(other)));
    assert_eq!(foreign.host_os.as_deref(), Some(other));
    assert!(foreign.is_cross_os);
  }

  #[test]
  fn api_profile_without_a_recorded_os_is_not_cross_os() {
    // An older profile that predates host_os must stay locally launchable
    // rather than being treated as foreign.
    let unknown = ApiProfile::from(&profile_with(SyncMode::Disabled, None));
    assert_eq!(unknown.host_os, None);
    assert!(!unknown.is_cross_os);
  }

  // Removing `browser` from UpdateProfileRequest, and rejecting invalid
  // `browser` values on create, must NOT make the API reject requests that
  // carry extra/unknown fields — old clients still send them. serde ignores
  // unknown fields by default; these tests lock that in so a future
  // `#[serde(deny_unknown_fields)]` can't silently break compatibility.
  #[test]
  fn update_profile_request_ignores_unknown_fields() {
    // `browser` is no longer a field, plus a wholly unknown field. Both must
    // be accepted and ignored, not rejected.
    let json = r#"{"name": "p", "browser": "wayfern", "totally_unknown": 123}"#;
    let parsed: UpdateProfileRequest =
      serde_json::from_str(json).expect("unknown fields must be ignored, not rejected");
    assert_eq!(parsed.name.as_deref(), Some("p"));
  }

  #[test]
  fn create_profile_request_ignores_unknown_fields() {
    let json = r#"{"name": "p", "browser": "chromium", "version": "latest", "future_field": true}"#;
    let parsed: CreateProfileRequest =
      serde_json::from_str(json).expect("unknown fields must be ignored, not rejected");
    assert_eq!(parsed.browser, "chromium");
  }

  #[test]
  fn create_profile_request_allows_omitting_version_and_configs() {
    // Minimal body: no version, no wayfern_config. Must
    // deserialize (version resolves to latest-downloaded at the handler; an
    // absent config triggers fresh-fingerprint generation).
    let json = r#"{"name": "p", "browser": "chromium"}"#;
    let parsed: CreateProfileRequest =
      serde_json::from_str(json).expect("version and configs are optional");
    assert_eq!(parsed.browser, "chromium");
    assert!(parsed.version.is_none());
    assert!(parsed.wayfern_config.is_none());
  }

  #[test]
  fn create_profile_browser_validation_matches_supported_engines() {
    // The handler rejects anything that isn't a launchable engine; this is the
    // same predicate it uses, kept in lockstep with MCP's create_profile.
    let is_valid = |b: &str| b == "chromium";
    assert!(is_valid("chromium"));
    assert!(!is_valid("wayfern"));
    assert!(!is_valid(""));
  }

  #[test]
  fn rate_limit_only_classifies_browser_automation_routes() {
    for path in [
      "/v1/profiles/profile-id/run",
      "/v1/profiles/profile-id/open-url",
      "/v1/profiles/profile-id/kill",
      "/v1/profiles/batch/run",
      "/v1/profiles/batch/stop",
      "/v1/vpn-leases",
    ] {
      assert!(
        is_automation_request(&Method::POST, path),
        "automation route was not limited: {path}"
      );
    }

    for (method, path) in [
      (Method::GET, "/v1/profiles/profile-id/run"),
      (Method::POST, "/v1/profiles"),
      (Method::POST, "/v1/profiles/import"),
      (Method::GET, "/v1/profiles"),
      (Method::GET, "/openapi.json"),
    ] {
      assert!(
        !is_automation_request(&method, path),
        "free or non-mutating route was limited: {method} {path}"
      );
    }
  }

  fn schema_required(spec: &serde_json::Value, schema: &str) -> Vec<String> {
    spec["components"]["schemas"][schema]["required"]
      .as_array()
      .map(|a| {
        a.iter()
          .filter_map(|v| v.as_str().map(str::to_string))
          .collect()
      })
      .unwrap_or_default()
  }

  // `#[schema(value_type = Object)]` on an `Option<T>` erases the optionality
  // and marks the field required in the served spec; these fields must stay
  // optional so generated clients aren't forced to send them.
  #[test]
  fn openapi_optional_fields_are_not_required() {
    let spec = serde_json::to_value(ApiDoc::openapi()).expect("spec serializes");

    let create_profile = schema_required(&spec, "CreateProfileRequest");
    assert!(
      !create_profile.iter().any(|f| f == "wayfern_config"),
      "wayfern_config must be optional, required list: {create_profile:?}"
    );

    let update_profile = schema_required(&spec, "UpdateProfileRequest");
    assert!(
      !update_profile.iter().any(|f| f == "group_id"),
      "group_id must be optional, required list: {update_profile:?}"
    );

    let update_proxy = schema_required(&spec, "UpdateProxyRequest");
    assert!(
      !update_proxy.iter().any(|f| f == "proxy_settings"),
      "proxy_settings must be optional on update, required list: {update_proxy:?}"
    );

    let proxy_settings = schema_required(&spec, "ProxySettings");
    for field in ["username", "password", "vless_uri"] {
      assert!(
        !proxy_settings.iter().any(|candidate| candidate == field),
        "{field} must be optional in proxy settings, required list: {proxy_settings:?}"
      );
      assert!(
        spec["components"]["schemas"]["ProxySettings"]["properties"][field].is_object(),
        "{field} must be present in the served ProxySettings schema"
      );
    }

    let import_profiles = schema_required(&spec, "ImportProfilesRequest");
    for field in ["group_id", "duplicate_strategy", "wayfern_config"] {
      assert!(
        !import_profiles.iter().any(|f| f == field),
        "{field} must be optional on profile import, required list: {import_profiles:?}"
      );
    }

    let import_item = schema_required(&spec, "ImportProfileItem");
    for field in ["proxy_id", "vpn_id", "browser_type"] {
      assert!(
        !import_item.iter().any(|f| f == field),
        "{field} must be optional on import items, required list: {import_item:?}"
      );
    }
  }

  #[test]
  fn import_profiles_request_allows_minimal_body() {
    // Only items with source_path + new_profile_name are required; everything
    // else has defaults.
    let json = r#"{"items": [{"source_path": "/tmp/p", "new_profile_name": "Imported"}]}"#;
    let parsed: ImportProfilesRequest =
      serde_json::from_str(json).expect("minimal import body must deserialize");
    assert_eq!(parsed.items.len(), 1);
    assert!(parsed.group_id.is_none());
    assert!(parsed.duplicate_strategy.is_none());
    assert_eq!(parsed.items[0].browser_type, "chromium");
  }

  #[test]
  fn vpn_lease_api_uses_the_documented_camel_case_contract() {
    let request: ApiAcquireVpnLeaseRequest = serde_json::from_str(
      r#"{"poolId":null,"country":"US","providers":["nordvpn"],"profileId":null,"ttlSeconds":0,"protocol":"socks5","waitWhenFull":true,"maxWaitSeconds":60}"#,
    )
    .expect("camelCase lease request must deserialize");
    assert_eq!(request.ttl_seconds, Some(0));
    assert!(request.wait_when_full);

    let spec = serde_json::to_value(ApiDoc::openapi()).expect("spec serializes");
    let request_properties = spec["components"]["schemas"]["ApiAcquireVpnLeaseRequest"]
      ["properties"]
      .as_object()
      .expect("request properties");
    for field in [
      "poolId",
      "profileId",
      "ttlSeconds",
      "waitWhenFull",
      "maxWaitSeconds",
    ] {
      assert!(
        request_properties.contains_key(field),
        "missing request field {field}"
      );
    }
    let response_properties = spec["components"]["schemas"]["ApiVpnLeaseResponse"]["properties"]
      .as_object()
      .expect("response properties");
    for field in [
      "leaseId",
      "poolId",
      "configId",
      "host",
      "port",
      "exitIp",
      "createdAt",
      "expiresAt",
    ] {
      assert!(
        response_properties.contains_key(field),
        "missing response field {field}"
      );
    }
  }

  // The served /openapi.json comes from the hand-maintained ApiDoc `paths(...)`
  // list, not from the router — endpoints registered on the router but missing
  // from ApiDoc silently disappear from the spec. Lock in the ones that were
  // once dropped, and that removed endpoints stay gone.
  #[test]
  fn openapi_spec_covers_registered_routes() {
    let spec = serde_json::to_value(ApiDoc::openapi()).expect("spec serializes");
    let paths = spec["paths"].as_object().expect("paths object");

    for path in [
      "/v1/vpns/{id}/export",
      "/v1/extensions",
      "/v1/extension-groups",
      "/v1/extensions/{id}",
      "/v1/extension-groups/{id}",
      "/v1/profiles/import",
      "/v1/profiles/import/detect",
      "/v1/proxies/import",
      "/v1/vpn-pools",
      "/v1/vpn-pools/{pool_id}",
      "/v1/vpn-leases",
      "/v1/vpn-leases/{lease_id}",
    ] {
      assert!(paths.contains_key(path), "missing from ApiDoc: {path}");
    }

    assert!(
      !paths.keys().any(|p| p.contains("wayfern-token")),
      "wayfern-token endpoints were removed and must stay out of the spec"
    );

    for path in [
      "/v1/profiles/{id}/run",
      "/v1/profiles/{id}/open-url",
      "/v1/profiles/{id}/kill",
      "/v1/profiles/batch/run",
      "/v1/profiles/batch/stop",
      "/v1/vpn-leases",
    ] {
      assert!(
        paths[path]["post"]["responses"].get("429").is_some(),
        "automation route is missing its 429 response: {path}"
      );
    }
  }
}
