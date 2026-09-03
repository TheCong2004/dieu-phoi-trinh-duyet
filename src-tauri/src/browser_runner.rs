use crate::browser::{Browser, ProxySettings};
use crate::cloud_auth::CLOUD_AUTH;
use crate::downloaded_browsers_registry::DownloadedBrowsersRegistry;
use crate::events;
use crate::profile::{BrowserProfile, ProfileManager};
use crate::proxy_manager::PROXY_MANAGER;
use crate::wayfern_manager::{WayfernConfig, WayfernManager};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ffi::OsString;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::Message;
use utoipa::ToSchema;

static PROFILE_LAUNCH_LOCKS: LazyLock<
  tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
static VPN_POOL_ROTATION_LOCKS: LazyLock<
  tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));
static TARGET_BINDING_SESSIONS: LazyLock<
  tokio::sync::Mutex<HashMap<String, TargetBindingSession>>,
> = LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

async fn lock_profile_launch(profile_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
  let lock = {
    let mut locks = PROFILE_LAUNCH_LOCKS.lock().await;
    locks
      .entry(profile_id.to_string())
      .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
      .clone()
  };
  lock.lock_owned().await
}

#[derive(Clone, Debug)]
pub enum LaunchUrlPolicy {
  AlwaysOpen(Option<String>),
  ColdStartOnly(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedLaunchIntent {
  Run,
  BindingPrepare,
}

#[derive(Clone, Debug)]
pub struct FlowordLaunchResult {
  pub profile: BrowserProfile,
  pub reused: bool,
  pub remote_debugging_port: Option<u16>,
  pub grok_target_id: Option<String>,
  pub grok_page_url: Option<String>,
  pub grok_target_reused: bool,
  pub target_selection_source: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct StopBrowserResult {
  pub profile_id: String,
  pub ok: bool,
  pub browser_engine: String,
  pub stopped_pid: u32,
  pub launch_generation: u64,
  pub graceful: bool,
}

const MANAGED_GROK_MARKER_KEY: &str = "__floword_managed_grok_target_v1";
const MANAGED_GROK_FRAGMENT_PREFIX: &str = "floword-managed=";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ManagedGrokMarker {
  version: u32,
  marker_id: String,
  profile_id: String,
  browser_pid: u32,
  launch_generation: u64,
  transaction_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct GrokLaunchLedger {
  launch_transaction_id: String,
  profile_id: String,
  browser_pid: Option<u32>,
  launch_generation: Option<u64>,
  target_stabilization_started_at: u64,
  preexisting_page_target_count: usize,
  preexisting_grok_target_count: usize,
  preexisting_target_id_hashes: Vec<String>,
  marker_matched_target_count: usize,
  created_target_count: usize,
  created_target_id_hashes: Vec<String>,
  navigated_blank_target_count: usize,
  selected_target_id: Option<String>,
  selection_path: Option<String>,
  closed_target_count: usize,
  final_grok_target_count: usize,
  snapshot_count: usize,
  stabilization_elapsed_ms: u64,
}

/// Sanitized, generation-scoped evidence for managed-target marker
/// persistence.  Marker values and URLs containing fragments are never
/// written to this journal; only hashes and normalized public URLs are kept.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct MarkerLifecycleCheckpoint {
  checkpoint: String,
  profile_id: String,
  generation: Option<u64>,
  target_id_hash: Option<String>,
  marker_hash: Option<String>,
  marker_present: bool,
  fragment_matched: bool,
  window_name_matched: bool,
  navigation_entry_matched: bool,
  normalized_url: Option<String>,
  document_lifecycle: String,
  elapsed_ms: u64,
}

#[derive(Debug, Clone)]
struct StartupGrokMigrationHint {
  profile_id: uuid::Uuid,
  target_id: String,
  browser_pid: u32,
  cdp_port: u16,
  launch_generation: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StartupGrokMigrationResult {
  pub profile_id: String,
  pub target_id_hash: String,
  pub marker_written: bool,
  pub marker_verified: bool,
  pub target_count_before: usize,
  pub target_count_after: usize,
  pub selection_path: String,
  pub created_target_count: usize,
  pub closed_target_count: usize,
  pub navigated_target_count: usize,
  pub reloaded_target_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TargetBindingCandidate {
  handle: String,
  target_id: String,
  target_id_hash: String,
  normalized_url: String,
  hostname: String,
  normalized_path: String,
  title_hash: String,
}

#[derive(Debug, Clone)]
struct TargetBindingSession {
  binding_session_id: String,
  profile_id: uuid::Uuid,
  browser_pid: u32,
  cdp_port: u16,
  launch_generation: u64,
  executable: String,
  user_data_dir: String,
  expires_at: u64,
  owns_browser: bool,
  candidates: Vec<TargetBindingCandidate>,
  previous_ledger: Option<ManagedTargetBindingLedger>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct TargetBindingCandidateResponse {
  pub handle: String,
  pub target_id_hash: String,
  pub url: String,
  pub hostname: String,
  pub normalized_path: String,
  pub title_hash: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct TargetBindingPrepareResponse {
  pub binding_required: bool,
  pub binding_session_id: String,
  pub browser_pid: u32,
  pub remote_debugging_port: u16,
  pub launch_generation: u64,
  pub candidate_count: usize,
  pub candidates: Vec<TargetBindingCandidateResponse>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetBindingConfirmResponse {
  pub profile_id: String,
  pub binding_session_id: String,
  pub target_id_hash: String,
  pub browser_pid: u32,
  pub cdp_port: u16,
  pub launch_generation: u64,
  pub lifecycle: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct TargetBindingAbortResponse {
  pub binding_session_id: String,
  pub lifecycle: String,
  pub browser_stopped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManagedTargetBindingLedger {
  profile_id: String,
  managed_target_binding_id: String,
  last_known_target_id: String,
  browser_pid: u32,
  cdp_port: u16,
  launch_generation: u64,
  managed_grok_page_url: String,
  binding_created_at: u64,
  binding_version: u32,
  lifecycle: String,
  executable: String,
  user_data_dir: String,
  expires_at: Option<u64>,
  /// The complete pending response is durable so a disconnected HTTP client
  /// can resume without minting new opaque candidate handles.
  #[serde(default)]
  candidates: Vec<TargetBindingCandidate>,
  /// Previous committed state used when an unrecoverable pending transition
  /// is reconciled after a runtime restart.
  #[serde(default)]
  previous_ledger: Option<Box<ManagedTargetBindingLedger>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChromiumLaunchReceipt {
  profile_id: String,
  browser_pid: u32,
  cdp_port: Option<u16>,
  launch_generation: u64,
  executable: String,
  user_data_dir: String,
  spawned_at: u64,
}

pub async fn binding_session_profile_id(binding_session_id: &str) -> Result<String, String> {
  if let Some(profile_id) = TARGET_BINDING_SESSIONS
    .lock()
    .await
    .get(binding_session_id)
    .map(|session| session.profile_id.to_string())
  {
    return Ok(profile_id);
  }
  find_durable_binding_session(ProfileManager::instance(), binding_session_id)
    .map(|(profile, _)| profile.id.to_string())
    .ok_or_else(|| "TARGET_BINDING_SESSION_NOT_FOUND".into())
}

#[cfg(test)]
pub(crate) async fn clear_target_binding_session_for_test(binding_session_id: &str) {
  TARGET_BINDING_SESSIONS
    .lock()
    .await
    .remove(binding_session_id);
}

fn startup_migration_hint_from_env() -> Result<Option<StartupGrokMigrationHint>, String> {
  let Some(profile_id) = std::env::var("FLOWORD_MANAGED_GROK_MIGRATION_PROFILE_ID")
    .ok()
    .filter(|value| !value.trim().is_empty())
  else {
    return Ok(None);
  };
  let required = |name: &str| {
    std::env::var(name)
      .map_err(|_| format!("{name} is required when managed Grok migration is requested"))
  };
  Ok(Some(StartupGrokMigrationHint {
    profile_id: uuid::Uuid::parse_str(&profile_id)
      .map_err(|_| "FLOWORD_MANAGED_GROK_MIGRATION_PROFILE_ID is invalid".to_string())?,
    target_id: required("FLOWORD_MANAGED_GROK_MIGRATION_TARGET_ID")?,
    browser_pid: required("FLOWORD_MANAGED_GROK_MIGRATION_BROWSER_PID")?
      .parse()
      .map_err(|_| "FLOWORD_MANAGED_GROK_MIGRATION_BROWSER_PID is invalid".to_string())?,
    cdp_port: required("FLOWORD_MANAGED_GROK_MIGRATION_CDP_PORT")?
      .parse()
      .map_err(|_| "FLOWORD_MANAGED_GROK_MIGRATION_CDP_PORT is invalid".to_string())?,
    launch_generation: required("FLOWORD_MANAGED_GROK_MIGRATION_LAUNCH_GENERATION")?
      .parse()
      .map_err(|_| "FLOWORD_MANAGED_GROK_MIGRATION_LAUNCH_GENERATION is invalid".to_string())?,
  }))
}

fn validate_startup_migration_identity(
  profile: &BrowserProfile,
  hint: &StartupGrokMigrationHint,
) -> Result<(), String> {
  if profile.id != hint.profile_id
    || profile.process_id != Some(hint.browser_pid)
    || profile.last_launch != Some(hint.launch_generation)
    || profile.browser != "chromium"
  {
    return Err("GROK_BROWSER_IDENTITY_CHANGED".into());
  }
  Ok(())
}

fn select_startup_migration_target(
  profile: &BrowserProfile,
  hint: &StartupGrokMigrationHint,
  pages: &[CdpPageTarget],
  markers: &HashMap<String, Option<ManagedGrokMarker>>,
  marker_id: &str,
) -> Result<(CdpPageTarget, &'static str), String> {
  validate_startup_migration_identity(profile, hint)?;
  let candidates = pages
    .iter()
    .filter(|page| is_exact_grok_page_url(&page.url))
    .cloned()
    .collect::<Vec<_>>();
  if let Some(exact) = candidates.iter().find(|page| page.id == hint.target_id) {
    if let Some(marker) = markers.get(&exact.id).and_then(Option::as_ref) {
      if !marker_matches_current_identity(marker, profile, marker_id) {
        return Err("GROK_TARGET_MARKER_CONFLICT".into());
      }
    }
    return Ok((exact.clone(), "EXACT_CURRENT_GENERATION_MAPPING"));
  }
  let marked = candidates
    .iter()
    .filter(|page| {
      markers
        .get(&page.id)
        .and_then(Option::as_ref)
        .is_some_and(|marker| marker_matches_current_identity(marker, profile, marker_id))
    })
    .collect::<Vec<_>>();
  match marked.as_slice() {
    [only] => return Ok(((*only).clone(), "DURABLE_MARKER_MATCH")),
    [] => {}
    _ => return Err("DUPLICATE_MANAGED_TARGET_MARKER".into()),
  }
  match candidates.as_slice() {
    [] => Err("GROK_TAB_NOT_FOUND".into()),
    [only] => match markers.get(&only.id).and_then(Option::as_ref) {
      None => Ok((only.clone(), "ADOPTED_SINGLE_EXISTING")),
      Some(marker) if marker_matches_current_identity(marker, profile, marker_id) => {
        Ok((only.clone(), "DURABLE_MARKER_MATCH"))
      }
      Some(_) => Err("GROK_TARGET_MARKER_CONFLICT".into()),
    },
    _ => Err("AMBIGUOUS_GROK_TAB".into()),
  }
}

fn startup_migration_marker_id(profile: &BrowserProfile) -> (String, bool) {
  match profile.managed_grok_marker_id.clone() {
    Some(marker_id) => (marker_id, false),
    None => (new_opaque_marker_id(), true),
  }
}

/// Generate an opaque binding token with at least 128 bits of entropy.  The
/// token is carried only in the local navigation fragment and durable ledger;
/// it is never emitted in API/log diagnostics.
fn new_opaque_marker_id() -> String {
  format!(
    "{}{}",
    uuid::Uuid::new_v4().simple(),
    uuid::Uuid::new_v4().simple()
  )
}

fn normalized_public_grok_url(url: &str) -> String {
  let Ok(mut parsed) = url::Url::parse(url) else {
    return url.to_string();
  };
  parsed.set_fragment(None);
  parsed.to_string()
}

fn managed_marker_fragment(marker_id: &str) -> String {
  format!("#{MANAGED_GROK_FRAGMENT_PREFIX}{marker_id}")
}

fn marker_id_from_fragment(url: &str) -> Option<String> {
  let parsed = url::Url::parse(url).ok()?;
  let fragment = parsed.fragment()?;
  let marker_id = fragment.strip_prefix(MANAGED_GROK_FRAGMENT_PREFIX)?;
  if marker_id.is_empty()
    || marker_id.len() > 128
    || !marker_id
      .bytes()
      .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
  {
    return None;
  }
  Some(marker_id.to_string())
}

fn persist_grok_ledger(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  ledger: &GrokLaunchLedger,
  phase: &str,
) -> Result<(), String> {
  let mut value = serde_json::to_value(ledger).map_err(|e| e.to_string())?;
  value["phase"] = serde_json::Value::String(phase.to_string());
  let profile_dir = profile.get_profile_data_path(&profile_manager.get_profiles_dir());
  std::fs::create_dir_all(&profile_dir).map_err(|e| e.to_string())?;
  let destination = profile_dir.join("floword-launch-ledger.json");
  let temporary = destination.with_extension("json.tmp");
  std::fs::write(
    &temporary,
    serde_json::to_vec_pretty(&value).map_err(|e| e.to_string())?,
  )
  .map_err(|e| e.to_string())?;
  atomic_replace_file(&temporary, &destination).map_err(|e| e.to_string())
}

/// Replace a durable JSON receipt without exposing a partially written file.
/// Windows does not let `rename` overwrite an existing file, so use the native
/// replace operation there; POSIX rename is already atomic and replacing.
fn atomic_replace_file(temporary: &Path, destination: &Path) -> std::io::Result<()> {
  #[cfg(windows)]
  {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
      #[link_name = "MoveFileExW"]
      fn move_file_ex_w(
        lp_existing_file_name: *const u16,
        lp_new_file_name: *const u16,
        dw_flags: u32,
      ) -> i32;
    }
    let from = temporary
      .as_os_str()
      .encode_wide()
      .chain(std::iter::once(0))
      .collect::<Vec<_>>();
    let to = destination
      .as_os_str()
      .encode_wide()
      .chain(std::iter::once(0))
      .collect::<Vec<_>>();
    // MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
    let moved = unsafe { move_file_ex_w(from.as_ptr(), to.as_ptr(), 0x1 | 0x8) };
    if moved == 0 {
      return Err(std::io::Error::last_os_error());
    }
    Ok(())
  }
  #[cfg(not(windows))]
  {
    std::fs::rename(temporary, destination)
  }
}

fn marker_lifecycle_journal_path(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> PathBuf {
  profile
    .get_profile_data_path(&profile_manager.get_profiles_dir())
    .join("floword-marker-lifecycle.json")
}

fn persist_marker_lifecycle_checkpoint(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  checkpoint: MarkerLifecycleCheckpoint,
) -> Result<(), String> {
  let destination = marker_lifecycle_journal_path(profile_manager, profile);
  if let Some(parent) = destination.parent() {
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
  }
  let mut entries = std::fs::read(&destination)
    .ok()
    .and_then(|bytes| serde_json::from_slice::<Vec<MarkerLifecycleCheckpoint>>(&bytes).ok())
    .unwrap_or_default();
  entries.push(checkpoint);
  if entries.len() > 128 {
    let keep_from = entries.len() - 128;
    entries.drain(..keep_from);
  }
  let temporary = destination.with_extension("json.tmp");
  std::fs::write(
    &temporary,
    serde_json::to_vec_pretty(&entries).map_err(|error| error.to_string())?,
  )
  .map_err(|error| error.to_string())?;
  atomic_replace_file(&temporary, &destination).map_err(|error| error.to_string())
}

fn marker_hash(marker_id: &str) -> String {
  target_id_hash(marker_id)
}

fn managed_target_binding_ledger_path(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> PathBuf {
  profile
    .get_profile_data_path(&profile_manager.get_profiles_dir())
    .join("floword-managed-target-binding.json")
}

fn persist_managed_target_binding_ledger(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  ledger: &ManagedTargetBindingLedger,
) -> Result<(), String> {
  let destination = managed_target_binding_ledger_path(profile_manager, profile);
  if let Some(parent) = destination.parent() {
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
  }
  let temporary = destination.with_extension("json.tmp");
  std::fs::write(
    &temporary,
    serde_json::to_vec_pretty(ledger).map_err(|error| error.to_string())?,
  )
  .map_err(|error| error.to_string())?;
  atomic_replace_file(&temporary, &destination).map_err(|error| error.to_string())
}

fn read_managed_target_binding_ledger(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> Option<ManagedTargetBindingLedger> {
  let path = managed_target_binding_ledger_path(profile_manager, profile);
  let bytes = std::fs::read(path).ok()?;
  serde_json::from_slice(&bytes).ok()
}

fn find_durable_binding_session(
  profile_manager: &'static ProfileManager,
  binding_session_id: &str,
) -> Option<(BrowserProfile, ManagedTargetBindingLedger)> {
  profile_manager
    .list_profiles()
    .ok()?
    .into_iter()
    .find_map(|profile| {
      let ledger = read_managed_target_binding_ledger(profile_manager, &profile)?;
      (ledger.managed_target_binding_id == binding_session_id
        && ledger.lifecycle == "BINDING_REQUIRED")
        .then_some((profile, ledger))
    })
}

fn target_binding_prepare_response_from_ledger(
  ledger: &ManagedTargetBindingLedger,
) -> TargetBindingPrepareResponse {
  let candidates = ledger
    .candidates
    .iter()
    .map(|candidate| TargetBindingCandidateResponse {
      handle: candidate.handle.clone(),
      target_id_hash: candidate.target_id_hash.clone(),
      url: candidate.normalized_url.clone(),
      hostname: candidate.hostname.clone(),
      normalized_path: candidate.normalized_path.clone(),
      title_hash: candidate.title_hash.clone(),
    })
    .collect::<Vec<_>>();
  TargetBindingPrepareResponse {
    binding_required: ledger.lifecycle == "BINDING_REQUIRED",
    binding_session_id: ledger.managed_target_binding_id.clone(),
    browser_pid: ledger.browser_pid,
    remote_debugging_port: ledger.cdp_port,
    launch_generation: ledger.launch_generation,
    candidate_count: candidates.len(),
    candidates,
  }
}

fn target_binding_prepare_response_from_session(
  session: &TargetBindingSession,
) -> TargetBindingPrepareResponse {
  let candidates = session
    .candidates
    .iter()
    .map(|candidate| TargetBindingCandidateResponse {
      handle: candidate.handle.clone(),
      target_id_hash: candidate.target_id_hash.clone(),
      url: candidate.normalized_url.clone(),
      hostname: candidate.hostname.clone(),
      normalized_path: candidate.normalized_path.clone(),
      title_hash: candidate.title_hash.clone(),
    })
    .collect::<Vec<_>>();
  TargetBindingPrepareResponse {
    binding_required: true,
    binding_session_id: session.binding_session_id.clone(),
    browser_pid: session.browser_pid,
    remote_debugging_port: session.cdp_port,
    launch_generation: session.launch_generation,
    candidate_count: candidates.len(),
    candidates,
  }
}

fn remove_managed_target_binding_ledger(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> Result<(), String> {
  let path = managed_target_binding_ledger_path(profile_manager, profile);
  match std::fs::remove_file(path) {
    Ok(()) => Ok(()),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
    Err(error) => Err(error.to_string()),
  }
}

fn chromium_launch_receipt_path(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> PathBuf {
  profile
    .get_profile_data_path(&profile_manager.get_profiles_dir())
    .join("floword-chromium-launch-receipt.json")
}

fn persist_chromium_launch_receipt(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  receipt: &ChromiumLaunchReceipt,
) -> Result<(), String> {
  let destination = chromium_launch_receipt_path(profile_manager, profile);
  if let Some(parent) = destination.parent() {
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
  }
  let temporary = destination.with_extension("json.tmp");
  std::fs::write(
    &temporary,
    serde_json::to_vec_pretty(receipt).map_err(|error| error.to_string())?,
  )
  .map_err(|error| error.to_string())?;
  atomic_replace_file(&temporary, &destination).map_err(|error| error.to_string())
}

fn remove_chromium_launch_receipt(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> Result<(), String> {
  let path = chromium_launch_receipt_path(profile_manager, profile);
  match std::fs::remove_file(path) {
    Ok(()) => Ok(()),
    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
    Err(error) => Err(error.to_string()),
  }
}

fn read_chromium_launch_receipt(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> Option<ChromiumLaunchReceipt> {
  let path = chromium_launch_receipt_path(profile_manager, profile);
  serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

fn chromium_receipt_matches_binding(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  ledger: &ManagedTargetBindingLedger,
) -> bool {
  read_chromium_launch_receipt(profile_manager, profile).is_some_and(|receipt| {
    receipt.profile_id == ledger.profile_id
      && receipt.browser_pid == ledger.browser_pid
      && receipt.cdp_port == Some(ledger.cdp_port)
      && receipt.launch_generation == ledger.launch_generation
      && normalized_identity_path(&receipt.executable)
        == normalized_identity_path(&ledger.executable)
      && normalized_identity_path(&receipt.user_data_dir)
        == normalized_identity_path(&ledger.user_data_dir)
  })
}

fn target_binding_prepare_failure_json(
  code: &str,
  stage: &str,
  message: &str,
  process_spawned: bool,
  rollback_attempted: bool,
  rollback_succeeded: bool,
) -> String {
  serde_json::json!({
    "code": code,
    "stage": if stage == "UNKNOWN" { "PREPARE" } else { stage },
    "message": message,
    "processSpawned": process_spawned,
    "rollbackAttempted": rollback_attempted,
    "rollbackSucceeded": rollback_succeeded,
    "retryable": false,
  })
  .to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StaleBindingReconcileAction {
  Keep,
  RemoveTemporary,
  RestoreCommitted,
  ClearOrphanMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingBindingRecoveryAction {
  Preserve,
  DeferIdentity,
  Rollback,
}

fn pending_binding_recovery_action(
  identity_matches: bool,
  receipt_matches: bool,
  expired: bool,
) -> PendingBindingRecoveryAction {
  if !identity_matches {
    PendingBindingRecoveryAction::DeferIdentity
  } else if receipt_matches && !expired {
    PendingBindingRecoveryAction::Preserve
  } else {
    PendingBindingRecoveryAction::Rollback
  }
}

/// Decide whether a durable binding ledger can be reconciled without making
/// assumptions about a browser that may have been restarted.  A profile PID
/// or an active lease is authoritative evidence that the transition may still
/// be live (or that its identity changed), so this helper deliberately keeps
/// the ledger in those cases.
fn stale_binding_reconcile_action(
  ledger: Option<&ManagedTargetBindingLedger>,
  profile_process_id: Option<u32>,
  active_lease: bool,
  active_binding_session: bool,
  previous_committed: bool,
  orphan_metadata: bool,
) -> StaleBindingReconcileAction {
  let Some(ledger) = ledger else {
    return if orphan_metadata
      && profile_process_id.is_none()
      && !active_lease
      && !active_binding_session
    {
      StaleBindingReconcileAction::ClearOrphanMetadata
    } else {
      StaleBindingReconcileAction::Keep
    };
  };
  if ledger.lifecycle != "BINDING_REQUIRED"
    || profile_process_id.is_some()
    || active_lease
    || active_binding_session
  {
    return StaleBindingReconcileAction::Keep;
  }
  if previous_committed {
    StaleBindingReconcileAction::RestoreCommitted
  } else {
    StaleBindingReconcileAction::RemoveTemporary
  }
}

fn has_managed_marker_metadata(profile: &BrowserProfile) -> bool {
  profile.managed_grok_marker_version.is_some()
    || profile.managed_grok_marker_id.is_some()
    || profile.managed_grok_marker_created_at.is_some()
    || profile.managed_grok_target_id.is_some()
    || profile.managed_grok_browser_pid.is_some()
    || profile.managed_grok_cdp_port.is_some()
    || profile.managed_grok_launch_generation.is_some()
}

fn clear_managed_marker_metadata(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
) -> Result<(), String> {
  if !has_managed_marker_metadata(profile) {
    return Ok(());
  }
  let mut cleared = profile.clone();
  cleared.managed_grok_marker_version = None;
  cleared.managed_grok_marker_id = None;
  cleared.managed_grok_marker_created_at = None;
  cleared.managed_grok_target_id = None;
  cleared.managed_grok_browser_pid = None;
  cleared.managed_grok_cdp_port = None;
  cleared.managed_grok_launch_generation = None;
  profile_manager
    .save_profile(&cleared)
    .map_err(|error| error.to_string())
}

fn restore_managed_marker_metadata(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  ledger: &ManagedTargetBindingLedger,
) -> Result<(), String> {
  let mut restored = profile.clone();
  restored.managed_grok_marker_version = Some(ledger.binding_version);
  restored.managed_grok_marker_id = Some(ledger.managed_target_binding_id.clone());
  restored.managed_grok_marker_created_at = Some(ledger.binding_created_at);
  restored.managed_grok_target_id = Some(ledger.last_known_target_id.clone());
  restored.managed_grok_browser_pid = Some(ledger.browser_pid);
  restored.managed_grok_cdp_port = Some(ledger.cdp_port);
  restored.managed_grok_launch_generation = Some(ledger.launch_generation);
  profile_manager
    .save_profile(&restored)
    .map_err(|error| error.to_string())
}

fn title_hash(title: &str) -> String {
  blake3::hash(title.as_bytes()).to_hex().to_string()[..16].to_string()
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct CdpPageTarget {
  pub(crate) id: String,
  pub(crate) url: String,
  pub(crate) websocket: String,
  pub(crate) title: String,
}

pub(crate) async fn list_cdp_pages(port: u16) -> Result<Vec<CdpPageTarget>, String> {
  let value: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/json/list"))
    .await
    .map_err(|e| e.to_string())?
    .json()
    .await
    .map_err(|e| e.to_string())?;
  Ok(
    value
      .as_array()
      .into_iter()
      .flatten()
      .filter(|item| item.get("type").and_then(|v| v.as_str()) == Some("page"))
      .filter_map(|item| {
        Some(CdpPageTarget {
          id: item.get("id")?.as_str()?.to_string(),
          url: item.get("url")?.as_str()?.to_string(),
          websocket: item.get("webSocketDebuggerUrl")?.as_str()?.to_string(),
          title: item
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_string(),
        })
      })
      .collect(),
  )
}

/// Create one page in an already running CFT. This is deliberately a CDP
/// operation: it never starts a browser process and never touches another
/// profile. The caller is responsible for recording the returned target as a
/// managed page before exposing it to automation.
pub(crate) async fn create_cdp_page(port: u16, url: &str) -> Result<CdpPageTarget, String> {
  let encoded = url::form_urlencoded::byte_serialize(url.as_bytes()).collect::<String>();
  let value: serde_json::Value = reqwest::Client::new()
    .put(format!("http://127.0.0.1:{port}/json/new?{encoded}"))
    .send()
    .await
    .map_err(|e| format!("CDP_PAGE_CREATE_FAILED: {e}"))?
    .json()
    .await
    .map_err(|e| format!("CDP_PAGE_CREATE_FAILED: {e}"))?;
  Ok(CdpPageTarget {
    id: value
      .get("id")
      .and_then(|v| v.as_str())
      .ok_or("CDP_PAGE_CREATE_FAILED")?
      .to_string(),
    url: value
      .get("url")
      .and_then(|v| v.as_str())
      .unwrap_or(url)
      .to_string(),
    websocket: value
      .get("webSocketDebuggerUrl")
      .and_then(|v| v.as_str())
      .ok_or("CDP_PAGE_CREATE_FAILED")?
      .to_string(),
    title: value
      .get("title")
      .and_then(|v| v.as_str())
      .unwrap_or_default()
      .to_string(),
  })
}

pub(crate) async fn close_cdp_page(port: u16, target_id: &str) -> Result<(), String> {
  if target_id.trim().is_empty()
    || !target_id
      .chars()
      .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
  {
    return Err("TARGET_ID_INVALID".into());
  }
  let response = reqwest::get(format!("http://127.0.0.1:{port}/json/close/{target_id}"))
    .await
    .map_err(|e| format!("CDP_PAGE_CLOSE_FAILED: {e}"))?;
  if response.status().is_success() {
    Ok(())
  } else {
    Err(format!("CDP_PAGE_CLOSE_FAILED: {}", response.status()))
  }
}

async fn navigate_cdp_target(target: &CdpPageTarget, url: &str) -> Result<(), String> {
  let command = async {
    let (mut socket, _) = tokio_tungstenite::connect_async(&target.websocket)
      .await
      .map_err(|e| format!("GROK_TARGET_NAVIGATION_FAILED: {e}"))?;
    socket
      .send(Message::Text(
        serde_json::json!({"id":1,"method":"Page.navigate","params":{"url":url}})
          .to_string()
          .into(),
      ))
      .await
      .map_err(|e| format!("GROK_TARGET_NAVIGATION_FAILED: {e}"))?;
    while let Some(message) = socket.next().await {
      let message = message.map_err(|e| format!("GROK_TARGET_NAVIGATION_FAILED: {e}"))?;
      let Message::Text(text) = message else {
        continue;
      };
      let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| format!("GROK_TARGET_NAVIGATION_FAILED: invalid CDP response: {e}"))?;
      if value.get("id").and_then(|id| id.as_i64()) != Some(1) {
        continue;
      }
      if let Some(error) = value.get("error") {
        return Err(format!("GROK_TARGET_NAVIGATION_FAILED: {error}"));
      }
      if let Some(error_text) = value
        .pointer("/result/errorText")
        .and_then(|error| error.as_str())
        .filter(|error| !error.is_empty())
      {
        return Err(format!("GROK_TARGET_NAVIGATION_FAILED: {error_text}"));
      }
      if value.get("result").is_none() {
        return Err("GROK_TARGET_NAVIGATION_UNKNOWN".into());
      }
      return Ok(());
    }
    Err("GROK_TARGET_NAVIGATION_UNKNOWN".into())
  };
  match tokio::time::timeout(Duration::from_secs(5), command).await {
    Ok(result) => result,
    Err(_) => Err("GROK_TARGET_NAVIGATION_UNKNOWN".into()),
  }
}

async fn wait_for_navigated_grok_target(
  port: u16,
  target_id: &str,
  deadline: tokio::time::Instant,
) -> Result<CdpPageTarget, String> {
  let mut previous_url: Option<String> = None;
  let mut stable_snapshots = 0usize;
  while tokio::time::Instant::now() < deadline {
    let pages = list_cdp_pages(port).await?;
    if let Some(target) = pages
      .iter()
      .find(|page| page.id == target_id && is_exact_grok_page_url(&page.url))
      .cloned()
    {
      if previous_url.as_deref() == Some(target.url.as_str()) {
        stable_snapshots += 1;
      } else {
        previous_url = Some(target.url.clone());
        stable_snapshots = 1;
      }
      if stable_snapshots >= 2 {
        return Ok(target);
      }
    } else {
      stable_snapshots = 0;
      previous_url = None;
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
  }
  Err("GROK_TARGET_NAVIGATION_FAILED".into())
}

async fn evaluate_cdp_target(
  target: &CdpPageTarget,
  expression: &str,
) -> Result<serde_json::Value, String> {
  let (mut socket, _) = tokio_tungstenite::connect_async(&target.websocket)
    .await
    .map_err(|e| e.to_string())?;
  socket
    .send(Message::Text(
      serde_json::json!({
        "id": 1,
        "method": "Runtime.evaluate",
        "params": {"expression": expression, "returnByValue": true, "awaitPromise": true}
      })
      .to_string()
      .into(),
    ))
    .await
    .map_err(|e| e.to_string())?;
  while let Some(message) = socket.next().await {
    let message = message.map_err(|e| e.to_string())?;
    let Message::Text(text) = message else {
      continue;
    };
    let value: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if value.get("id").and_then(|id| id.as_i64()) == Some(1) {
      if let Some(description) = value
        .get("result")
        .and_then(|result| result.get("exceptionDetails"))
        .and_then(|details| details.get("text"))
        .and_then(|text| text.as_str())
      {
        return Err(description.to_string());
      }
      return Ok(
        value
          .pointer("/result/result/value")
          .cloned()
          .unwrap_or(serde_json::Value::Null),
      );
    }
  }
  Err("CDP_EVALUATE_DISCONNECTED".into())
}

fn is_chrome_for_testing_profile(profile: &BrowserProfile) -> bool {
  crate::browser::is_chrome_for_testing_alias(&profile.browser)
}

fn normalized_identity_path(value: &str) -> String {
  value
    .trim_matches('"')
    .replace('/', "\\")
    .to_ascii_lowercase()
}

fn command_has_identity_arg(cmd: &[OsString], prefix: &str, expected: &Path) -> bool {
  let expected = normalized_identity_path(&format!("{prefix}={}", expected.display()));
  cmd
    .iter()
    .any(|arg| normalized_identity_path(&arg.to_string_lossy()) == expected)
}

fn command_has_port_arg(cmd: &[OsString], port: u16) -> bool {
  let expected = format!("--remote-debugging-port={port}");
  cmd
    .iter()
    .any(|arg| arg.to_string_lossy().eq_ignore_ascii_case(&expected))
}

fn chromium_process_identity_matches(
  profile: &BrowserProfile,
  profiles_dir: &Path,
  expected_executable: &Path,
) -> Result<(u32, u16, u64), String> {
  if !is_chrome_for_testing_profile(profile) {
    return Err("BROWSER_SESSION_IDENTITY_MISMATCH".into());
  }
  let pid = profile
    .process_id
    .filter(|pid| *pid != 0)
    .ok_or_else(|| "BROWSER_SESSION_IDENTITY_MISMATCH".to_string())?;
  if profile.managed_grok_browser_pid != Some(pid)
    || profile
      .managed_grok_target_id
      .as_deref()
      .is_none_or(str::is_empty)
    || profile.managed_grok_cdp_port.is_none_or(|port| port == 0)
    || profile.managed_grok_launch_generation != profile.last_launch
  {
    return Err("BROWSER_SESSION_IDENTITY_MISMATCH".into());
  }
  let port = profile.managed_grok_cdp_port.unwrap();
  let generation = profile
    .managed_grok_launch_generation
    .ok_or_else(|| "BROWSER_SESSION_IDENTITY_MISMATCH".to_string())?;
  let profile_path = crate::ephemeral_dirs::get_effective_profile_path(profile, profiles_dir)
    .join("floword-chromium");
  let system = sysinfo::System::new_with_specifics(
    sysinfo::RefreshKind::nothing().with_processes(sysinfo::ProcessRefreshKind::everything()),
  );
  let process = system
    .process(sysinfo::Pid::from(pid as usize))
    .ok_or_else(|| "BROWSER_SESSION_IDENTITY_MISMATCH".to_string())?;
  let actual_executable = process
    .exe()
    .ok_or_else(|| "BROWSER_SESSION_IDENTITY_MISMATCH".to_string())?;
  if normalized_identity_path(&actual_executable.to_string_lossy())
    != normalized_identity_path(&expected_executable.to_string_lossy())
    || normalized_identity_path(&actual_executable.to_string_lossy())
      .contains("google\\chrome\\application")
    || !command_has_identity_arg(process.cmd(), "--user-data-dir", &profile_path)
    || !command_has_port_arg(process.cmd(), port)
  {
    return Err("BROWSER_SESSION_IDENTITY_MISMATCH".into());
  }
  Ok((pid, port, generation))
}

/// Validate a freshly spawned CFT before its managed Grok target has been
/// committed.  This deliberately uses only the launch receipt (PID, port and
/// generation); the durable target mapping is not required during rollback.
fn chromium_spawn_identity_matches(
  profile: &BrowserProfile,
  profiles_dir: &Path,
  pid: u32,
  port: u16,
  expected_executable: &Path,
) -> bool {
  if !is_chrome_for_testing_profile(profile)
    || profile.process_id != Some(pid)
    || profile.last_launch.is_none()
    || port == 0
  {
    return false;
  }
  let profile_path = crate::ephemeral_dirs::get_effective_profile_path(profile, profiles_dir)
    .join("floword-chromium");
  let system = sysinfo::System::new_with_specifics(
    sysinfo::RefreshKind::nothing().with_processes(sysinfo::ProcessRefreshKind::everything()),
  );
  let Some(process) = system.process(sysinfo::Pid::from(pid as usize)) else {
    return false;
  };
  let Some(actual_executable) = process.exe() else {
    return false;
  };
  normalized_identity_path(&actual_executable.to_string_lossy())
    == normalized_identity_path(&expected_executable.to_string_lossy())
    && !normalized_identity_path(&actual_executable.to_string_lossy())
      .contains("google\\chrome\\application")
    && command_has_identity_arg(process.cmd(), "--user-data-dir", &profile_path)
    && command_has_port_arg(process.cmd(), port)
}

async fn rollback_chromium_launch(profile: &mut BrowserProfile, port: u16) -> Result<bool, String> {
  let pid = profile
    .process_id
    .filter(|pid| *pid != 0)
    .ok_or_else(|| "BROWSER_SESSION_NOT_MANAGED".to_string())?;
  let profiles_dir = ProfileManager::instance().get_profiles_dir();
  let port = if port == 0 {
    let port_path = crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
      .join("floword-chromium")
      .join(".floword-cdp-port");
    std::fs::read_to_string(port_path)
      .ok()
      .and_then(|value| value.trim().parse().ok())
      .ok_or_else(|| "BROWSER_SESSION_NOT_MANAGED".to_string())?
  } else {
    port
  };
  let executable = crate::browser::ChromiumBrowser::resolve_executable()
    .map_err(|_| "BROWSER_SESSION_IDENTITY_MISMATCH".to_string())?;
  if !chromium_spawn_identity_matches(profile, &profiles_dir, pid, port, &executable) {
    return Err("BROWSER_SESSION_IDENTITY_MISMATCH".into());
  }
  let checkpoint_started = tokio::time::Instant::now();
  if let Some(marker_id) = profile.managed_grok_marker_id.as_deref() {
    if let Ok(pages) = list_cdp_pages(port).await {
      if let Some(target) = pages
        .into_iter()
        .find(|target| Some(target.id.as_str()) == profile.managed_grok_target_id.as_deref())
      {
        capture_marker_checkpoint(
          ProfileManager::instance(),
          profile,
          &target,
          marker_id,
          profile.last_launch,
          "MARKER_STABLE_BEFORE_STOP",
          checkpoint_started,
        )
        .await;
      }
    }
  }
  let graceful = close_cdp_browser(port).await.is_ok();
  let mut stopped = wait_for_process_exit(pid, Duration::from_secs(5)).await;
  if !stopped {
    if !chromium_spawn_identity_matches(profile, &profiles_dir, pid, port, &executable) {
      return Err("BROWSER_SESSION_IDENTITY_MISMATCH".into());
    }
    #[cfg(target_os = "windows")]
    crate::platform_browser::windows::kill_browser_process_impl(pid)
      .await
      .map_err(|e| e.to_string())?;
    #[cfg(target_os = "macos")]
    crate::platform_browser::macos::kill_browser_process_impl(
      pid,
      Some(
        &crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
          .join("floword-chromium")
          .to_string_lossy(),
      ),
    )
    .await
    .map_err(|e| e.to_string())?;
    #[cfg(target_os = "linux")]
    crate::platform_browser::linux::kill_browser_process_impl(
      pid,
      Some(
        &crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
          .join("floword-chromium")
          .to_string_lossy(),
      ),
    )
    .await
    .map_err(|e| e.to_string())?;
    stopped = wait_for_process_exit(pid, Duration::from_secs(3)).await;
  }
  if !stopped {
    return Err("BROWSER_STOP_FAILED".into());
  }
  profile.process_id = None;
  profile.managed_grok_target_id = None;
  profile.managed_grok_browser_pid = None;
  profile.managed_grok_cdp_port = None;
  profile.managed_grok_launch_generation = None;
  ProfileManager::instance()
    .save_profile(profile)
    .map_err(|e| e.to_string())?;
  remove_chromium_launch_receipt(ProfileManager::instance(), profile)?;
  if let Some(marker_id) = profile.managed_grok_marker_id.as_deref() {
    capture_session_store_checkpoint(
      ProfileManager::instance(),
      profile,
      marker_id,
      profile.last_launch,
      checkpoint_started,
    );
  }
  Ok(graceful)
}

async fn close_cdp_browser(port: u16) -> Result<(), String> {
  let value: serde_json::Value = reqwest::get(format!("http://127.0.0.1:{port}/json/version"))
    .await
    .map_err(|e| format!("CDP_BROWSER_CLOSE_FAILED: {e}"))?
    .json()
    .await
    .map_err(|e| format!("CDP_BROWSER_CLOSE_FAILED: {e}"))?;
  let websocket = value
    .get("webSocketDebuggerUrl")
    .and_then(|value| value.as_str())
    .ok_or_else(|| "CDP_BROWSER_CLOSE_FAILED: missing browser websocket".to_string())?;
  let command = async {
    let (mut socket, _) = tokio_tungstenite::connect_async(websocket)
      .await
      .map_err(|e| format!("CDP_BROWSER_CLOSE_FAILED: {e}"))?;
    socket
      .send(Message::Text(
        serde_json::json!({"id":1,"method":"Browser.close"})
          .to_string()
          .into(),
      ))
      .await
      .map_err(|e| format!("CDP_BROWSER_CLOSE_FAILED: {e}"))?;
    Ok::<(), String>(())
  };
  tokio::time::timeout(Duration::from_secs(5), command)
    .await
    .map_err(|_| "CDP_BROWSER_CLOSE_FAILED: timeout".to_string())??;
  Ok(())
}

async fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
  let deadline = tokio::time::Instant::now() + timeout;
  while tokio::time::Instant::now() < deadline {
    let system = sysinfo::System::new_with_specifics(
      sysinfo::RefreshKind::nothing().with_processes(sysinfo::ProcessRefreshKind::everything()),
    );
    if system.process(sysinfo::Pid::from(pid as usize)).is_none() {
      return true;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
  let system = sysinfo::System::new_with_specifics(
    sysinfo::RefreshKind::nothing().with_processes(sysinfo::ProcessRefreshKind::everything()),
  );
  system.process(sysinfo::Pid::from(pid as usize)).is_none()
}

fn is_grok_url(url: &str) -> bool {
  url::Url::parse(url).ok().is_some_and(|u| {
    u.host_str()
      .is_some_and(|h| h == "grok.com" || h.ends_with(".grok.com"))
  })
}

fn is_exact_grok_page_url(url: &str) -> bool {
  url::Url::parse(url)
    .ok()
    .is_some_and(|parsed| parsed.scheme() == "https" && parsed.host_str() == Some("grok.com"))
}

fn is_blank_url(url: &str) -> bool {
  url.is_empty()
    || matches!(
      url,
      "about:blank" | "chrome://newtab/" | "chrome://new-tab-page/"
    )
}

fn managed_mapping_is_stale(profile: &BrowserProfile, port: u16) -> bool {
  profile.managed_grok_browser_pid != profile.process_id
    || profile.managed_grok_cdp_port != Some(port)
    || profile.managed_grok_launch_generation != profile.last_launch
}

fn sanitize_chromium_launch_url(url: Option<String>, cold_start_floword: bool) -> Option<String> {
  if cold_start_floword && url.as_deref().is_some_and(is_grok_url) {
    None
  } else {
    url
  }
}

fn target_id_hash(id: &str) -> String {
  blake3::hash(id.as_bytes()).to_hex().to_string()[..16].to_string()
}

async fn stabilized_cdp_pages(
  port: u16,
  timeout: Duration,
) -> Result<(Vec<CdpPageTarget>, usize, u64), String> {
  let started = tokio::time::Instant::now();
  let deadline = started + timeout;
  let mut previous: Option<Vec<(String, String)>> = None;
  let mut stable_snapshots = 0usize;
  let mut snapshots = 0usize;
  let mut last_error: Option<String> = None;
  while tokio::time::Instant::now() < deadline {
    let pages = match list_cdp_pages(port).await {
      Ok(pages) => pages,
      Err(error) => {
        // A freshly spawned CFT may expose its port before /json/list is
        // ready. Keep the bounded deadline meaningful by retrying transient
        // CDP readiness errors instead of failing on the first connection.
        last_error = Some(error);
        tokio::time::sleep(Duration::from_millis(250)).await;
        continue;
      }
    };
    snapshots += 1;
    let fingerprint = cdp_page_fingerprint(&pages);
    if previous.as_ref() == Some(&fingerprint) {
      stable_snapshots += 1;
    } else {
      stable_snapshots = 1;
      previous = Some(fingerprint);
    }
    if stable_snapshots >= 2 {
      return Ok((pages, snapshots, started.elapsed().as_millis() as u64));
    }
    tokio::time::sleep(Duration::from_millis(250)).await;
  }
  Err(last_error.unwrap_or_else(|| "GROK_TARGET_SET_UNSTABLE".into()))
}

/// Wait for the browser's CDP HTTP surface to become usable.  Binding
/// preparation deliberately probes the browser endpoint first and the target
/// list second so a spawned process is never reported as ready merely because
/// its port was allocated.
async fn wait_for_cdp_ready(port: u16, timeout: Duration) -> Result<(), String> {
  let deadline = tokio::time::Instant::now() + timeout;
  let client = reqwest::Client::builder()
    .connect_timeout(Duration::from_millis(500))
    .timeout(Duration::from_secs(1))
    .build()
    .map_err(|error| format!("TARGET_BINDING_CDP_READINESS_FAILED: {error}"))?;
  let version_url = format!("http://127.0.0.1:{port}/json/version");
  let list_url = format!("http://127.0.0.1:{port}/json/list");
  while tokio::time::Instant::now() < deadline {
    let version_ready = client
      .get(&version_url)
      .send()
      .await
      .is_ok_and(|response| response.status().is_success());
    if version_ready {
      let list_ready = client
        .get(&list_url)
        .send()
        .await
        .is_ok_and(|response| response.status().is_success());
      if list_ready {
        return Ok(());
      }
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
  }
  Err(format!(
    "TARGET_BINDING_CDP_READINESS_TIMEOUT: CDP endpoints did not become ready on port {port}"
  ))
}

fn cdp_page_fingerprint(pages: &[CdpPageTarget]) -> Vec<(String, String)> {
  pages
    .iter()
    .map(|page| (page.id.clone(), page.url.clone()))
    .collect()
}

async fn read_marker(target: &CdpPageTarget) -> Result<Option<ManagedGrokMarker>, String> {
  let key = serde_json::to_string(MANAGED_GROK_MARKER_KEY).unwrap();
  let expression = format!(
    "(() => {{ try {{ const value=JSON.parse(window.name||'{{}}')[{key}]; return value && typeof value === 'object' ? value : null; }} catch (_) {{ return null; }} }})()"
  );
  let value = evaluate_cdp_target(target, &expression).await?;
  if value.is_null() {
    return Ok(None);
  }
  serde_json::from_value(value)
    .map(Some)
    .map_err(|_| "MANAGED_GROK_MARKER_INVALID".to_string())
}

async fn write_marker(target: &CdpPageTarget, marker: &ManagedGrokMarker) -> Result<bool, String> {
  let key = serde_json::to_string(MANAGED_GROK_MARKER_KEY).unwrap();
  let value = serde_json::to_string(marker).map_err(|error| error.to_string())?;
  let fragment = serde_json::to_string(&managed_marker_fragment(&marker.marker_id))
    .map_err(|error| error.to_string())?;
  let expression = format!(
    "(() => {{ let state={{}}; try {{ state=JSON.parse(window.name||'{{}}'); }} catch (_) {{ state={{}}; }} state[{key}]={value}; window.name=JSON.stringify(state); const url=new URL(window.location.href); url.hash={fragment}; history.replaceState(history.state, '', url.toString()); return true; }})()"
  );
  if evaluate_cdp_target(target, &expression).await?.as_bool() != Some(true) {
    return Ok(false);
  }
  Ok(
    read_marker(target).await?.as_ref() == Some(marker)
      && read_marker_binding_id(target).await?.as_deref() == Some(marker.marker_id.as_str()),
  )
}

async fn clear_marker(target: &CdpPageTarget) -> Result<(), String> {
  let key = serde_json::to_string(MANAGED_GROK_MARKER_KEY).unwrap();
  let expression = format!(
    "(() => {{ let state={{}}; try {{ state=JSON.parse(window.name||'{{}}'); }} catch (_) {{ state={{}}; }} delete state[{key}]; window.name=JSON.stringify(state); const url=new URL(window.location.href); if (url.hash.startsWith('#{MANAGED_GROK_FRAGMENT_PREFIX}')) {{ url.hash=''; history.replaceState(history.state, '', url.toString()); }} return true; }})()"
  );
  if evaluate_cdp_target(target, &expression).await?.as_bool() != Some(true) {
    return Err("MANAGED_GROK_MARKER_CLEAR_FAILED".into());
  }
  if read_marker(target).await?.is_some() || read_marker_binding_id(target).await?.is_some() {
    return Err("MANAGED_GROK_MARKER_CLEAR_FAILED".into());
  }
  Ok(())
}

async fn read_marker_binding_id(target: &CdpPageTarget) -> Result<Option<String>, String> {
  let expression = format!(
    "(() => {{ const hash=window.location.hash||''; const prefix='#{MANAGED_GROK_FRAGMENT_PREFIX}'; if (!hash.startsWith(prefix)) return null; const value=hash.slice(prefix.length); return /^[A-Za-z0-9_-]{{1,128}}$/.test(value) ? value : null; }})()"
  );
  let value = evaluate_cdp_target(target, &expression).await?;
  Ok(value.as_str().map(ToOwned::to_owned))
}

async fn read_target_href(target: &CdpPageTarget) -> Result<String, String> {
  let value = evaluate_cdp_target(target, "location.href").await?;
  value
    .as_str()
    .map(ToOwned::to_owned)
    .ok_or_else(|| "MANAGED_GROK_TARGET_URL_UNREADABLE".into())
}

async fn replace_marker_url(target: &CdpPageTarget, marker_id: &str) -> Result<bool, String> {
  let fragment = serde_json::to_string(&managed_marker_fragment(marker_id))
    .map_err(|error| error.to_string())?;
  let expression = format!(
    "(() => {{ const current=new URL(location.href); const existing=current.hash||''; if (existing && !existing.startsWith('#{MANAGED_GROK_FRAGMENT_PREFIX}')) return false; current.hash={fragment}; history.replaceState(history.state, '', current.toString()); return true; }})()"
  );
  Ok(evaluate_cdp_target(target, &expression).await?.as_bool() == Some(true))
}

async fn marker_carriers_match(
  target: &CdpPageTarget,
  marker: &ManagedGrokMarker,
) -> Result<bool, String> {
  Ok(
    read_marker(target).await?.as_ref() == Some(marker)
      && read_marker_binding_id(target).await?.as_deref() == Some(marker.marker_id.as_str())
      && navigation_history_matches_marker(target, &marker.marker_id).await?,
  )
}

fn navigation_history_current_entry_matches(value: &serde_json::Value, marker_id: &str) -> bool {
  let entries = value
    .pointer("/result/entries")
    .and_then(|entries| entries.as_array());
  let current_index = value
    .pointer("/result/currentIndex")
    .and_then(|index| index.as_u64())
    .and_then(|index| usize::try_from(index).ok());
  let expected_fragment = managed_marker_fragment(marker_id);
  current_index
    .and_then(|index| entries.and_then(|entries| entries.get(index)))
    .and_then(|entry| entry.get("url"))
    .and_then(|url| url.as_str())
    .and_then(|url| url::Url::parse(url).ok())
    .and_then(|parsed| parsed.fragment().map(str::to_string))
    .is_some_and(|fragment| fragment == expected_fragment[1..])
}

async fn restore_marker_carriers(
  target: &CdpPageTarget,
  previous_marker: Option<&ManagedGrokMarker>,
  previous_href: &str,
) -> Result<(), String> {
  let key = serde_json::to_string(MANAGED_GROK_MARKER_KEY).unwrap();
  let previous_value = previous_marker
    .map(serde_json::to_value)
    .transpose()
    .map_err(|error| error.to_string())?
    .unwrap_or(serde_json::Value::Null);
  let href = serde_json::to_string(previous_href).map_err(|error| error.to_string())?;
  let expression = format!(
    "(() => {{ let state={{}}; try {{ state=JSON.parse(window.name||'{{}}'); }} catch (_) {{ state={{}}; }} const previous={previous_value}; if (previous === null) delete state[{key}]; else state[{key}]=previous; window.name=JSON.stringify(state); history.replaceState(history.state, '', {href}); return true; }})()"
  );
  if evaluate_cdp_target(target, &expression).await?.as_bool() != Some(true) {
    return Err("MANAGED_GROK_MARKER_ROLLBACK_FAILED".into());
  }
  Ok(())
}

async fn navigation_history_matches_marker(
  target: &CdpPageTarget,
  marker_id: &str,
) -> Result<bool, String> {
  let (mut socket, _) = tokio_tungstenite::connect_async(&target.websocket)
    .await
    .map_err(|error| error.to_string())?;
  socket
    .send(Message::Text(
      serde_json::json!({"id":1,"method":"Page.getNavigationHistory"})
        .to_string()
        .into(),
    ))
    .await
    .map_err(|error| error.to_string())?;
  while let Some(message) = socket.next().await {
    let Message::Text(text) = message.map_err(|error| error.to_string())? else {
      continue;
    };
    let value: serde_json::Value =
      serde_json::from_str(&text).map_err(|error| error.to_string())?;
    if value.get("id").and_then(|id| id.as_i64()) != Some(1) {
      continue;
    }
    return Ok(navigation_history_current_entry_matches(&value, marker_id));
  }
  Ok(false)
}

async fn capture_marker_checkpoint(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  target: &CdpPageTarget,
  marker_id: &str,
  generation: Option<u64>,
  checkpoint: &str,
  started: tokio::time::Instant,
) {
  let marker = read_marker(target).await.ok().flatten();
  let fragment_matched = read_marker_binding_id(target)
    .await
    .ok()
    .flatten()
    .is_some_and(|value| value == marker_id);
  let navigation_entry_matched = navigation_history_matches_marker(target, marker_id)
    .await
    .unwrap_or(false);
  let record = MarkerLifecycleCheckpoint {
    checkpoint: checkpoint.to_string(),
    profile_id: profile.id.to_string(),
    generation,
    target_id_hash: Some(target_id_hash(&target.id)),
    marker_hash: Some(marker_hash(marker_id)),
    marker_present: marker.is_some(),
    fragment_matched,
    window_name_matched: marker.is_some(),
    navigation_entry_matched,
    normalized_url: Some(normalized_public_grok_url(&target.url)),
    document_lifecycle: "active".into(),
    elapsed_ms: started.elapsed().as_millis() as u64,
  };
  if let Err(error) = persist_marker_lifecycle_checkpoint(profile_manager, profile, record) {
    log::warn!("Failed to persist marker lifecycle checkpoint: {error}");
  }
}

fn bytes_contain_exact_marker(bytes: &[u8], marker_id: &str) -> bool {
  let ascii = String::from_utf8_lossy(bytes);
  let utf16 = String::from_utf16_lossy(
    &bytes
      .chunks_exact(2)
      .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
      .collect::<Vec<_>>(),
  );
  [
    marker_id.to_string(),
    format!("floword-managed={marker_id}"),
    format!("floword-managed%3D{marker_id}"),
  ]
  .iter()
  .any(|variant| ascii.contains(variant) || utf16.contains(variant))
}

fn session_store_exact_marker_present(profile: &BrowserProfile, marker_id: &str) -> bool {
  let root = crate::ephemeral_dirs::get_effective_profile_path(
    profile,
    &ProfileManager::instance().get_profiles_dir(),
  )
  .join("floword-chromium");
  let mut files = Vec::new();
  fn collect_files(dir: &Path, files: &mut Vec<(Option<SystemTime>, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
      return;
    };
    for entry in entries.flatten() {
      let path = entry.path();
      if path.is_dir() {
        collect_files(&path, files);
      } else if path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.starts_with("Tabs_") || name.starts_with("Session_"))
      {
        if let Ok(metadata) = entry.metadata() {
          files.push((metadata.modified().ok(), path));
        }
      }
    }
  }
  collect_files(&root, &mut files);
  files.sort_by_key(|right| std::cmp::Reverse(right.0));
  files.into_iter().take(2).any(|(_, path)| {
    let Ok(bytes) = std::fs::read(path) else {
      return false;
    };
    bytes_contain_exact_marker(&bytes, marker_id)
  })
}

fn capture_session_store_checkpoint(
  profile_manager: &'static ProfileManager,
  profile: &BrowserProfile,
  marker_id: &str,
  generation: Option<u64>,
  started: tokio::time::Instant,
) {
  let present = session_store_exact_marker_present(profile, marker_id);
  let record = MarkerLifecycleCheckpoint {
    checkpoint: "SESSION_STORE_AFTER_STOP".into(),
    profile_id: profile.id.to_string(),
    generation,
    target_id_hash: profile
      .managed_grok_target_id
      .as_deref()
      .map(target_id_hash),
    marker_hash: Some(marker_hash(marker_id)),
    marker_present: present,
    fragment_matched: present,
    window_name_matched: present,
    navigation_entry_matched: false,
    normalized_url: None,
    document_lifecycle: "stopped".into(),
    elapsed_ms: started.elapsed().as_millis() as u64,
  };
  if let Err(error) = persist_marker_lifecycle_checkpoint(profile_manager, profile, record) {
    log::warn!("Failed to persist session-store marker checkpoint: {error}");
  }
}

fn marker_belongs_to_profile(
  marker: &ManagedGrokMarker,
  profile: &BrowserProfile,
  marker_id: &str,
) -> bool {
  marker.version == 1
    && marker.marker_id == marker_id
    && marker.profile_id == profile.id.to_string()
}

fn marker_matches_current_identity(
  marker: &ManagedGrokMarker,
  profile: &BrowserProfile,
  marker_id: &str,
) -> bool {
  marker_belongs_to_profile(marker, profile, marker_id)
    && profile.process_id == Some(marker.browser_pid)
    && profile.last_launch == Some(marker.launch_generation)
}

fn marker_matches_binding_ledger(
  marker: &ManagedGrokMarker,
  profile: &BrowserProfile,
  ledger: Option<&ManagedTargetBindingLedger>,
) -> bool {
  let Some(ledger) = ledger else {
    return false;
  };
  ledger.lifecycle == "COMMITTED"
    && ledger.profile_id == profile.id.to_string()
    && ledger.managed_target_binding_id == marker.marker_id
    && marker.version == ledger.binding_version
    && marker.profile_id == ledger.profile_id
}

/// Reconcile durable binding transitions left behind by a runtime restart.
/// A pending session with a valid receipt, exact process identity and at least
/// one live candidate is intentionally preserved so a client can resume it;
/// only an unrecoverable transition is rolled back/cleared.
pub async fn reconcile_pending_binding_sessions_on_startup() -> Result<(), String> {
  let profile_manager = ProfileManager::instance();
  let profiles = profile_manager
    .list_profiles()
    .map_err(|error| error.to_string())?;
  for profile in profiles {
    let Some(ledger) = read_managed_target_binding_ledger(profile_manager, &profile) else {
      continue;
    };
    if ledger.lifecycle != "BINDING_REQUIRED" {
      continue;
    }
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs();
    let identity_matches = chromium_spawn_identity_matches(
      &profile,
      &profile_manager.get_profiles_dir(),
      ledger.browser_pid,
      ledger.cdp_port,
      Path::new(&ledger.executable),
    ) && profile.last_launch == Some(ledger.launch_generation);
    let receipt_matches = chromium_receipt_matches_binding(profile_manager, &profile, &ledger);
    let expired = ledger.expires_at.is_some_and(|expires| now > expires);
    // A matching receipt is durable proof that Donut owns this transition.
    // Preserve it across restart even if CDP target enumeration is temporarily
    // unavailable; the read-only resume endpoint can reuse the exact handles.
    match pending_binding_recovery_action(identity_matches, receipt_matches, expired) {
      PendingBindingRecoveryAction::Preserve => {
        log::info!(
          "PENDING_BINDING_SESSION_RESUMABLE profile={} binding_session_id_hash={}",
          profile.id,
          target_id_hash(&ledger.managed_target_binding_id)
        );
        continue;
      }
      PendingBindingRecoveryAction::DeferIdentity => {
        log::warn!(
          "PENDING_BINDING_SESSION_RECONCILIATION_DEFERRED profile={} reason=IDENTITY_UNCERTAIN",
          profile.id
        );
        continue;
      }
      PendingBindingRecoveryAction::Rollback => {}
    }
    let mut rollback = false;
    let mut rollback_error = None;
    if identity_matches {
      let mut rollback_profile = profile.clone();
      match rollback_chromium_launch(&mut rollback_profile, ledger.cdp_port).await {
        Ok(_) => rollback = true,
        Err(error) => rollback_error = Some(error),
      }
    }
    if let Some(error) = rollback_error {
      log::error!(
        "UNRECOVERABLE_BINDING_SESSION_RECONCILED profile={} action=ROLLBACK_FAILED error={}",
        profile.id,
        error
      );
      return Err(error);
    }
    if let Some(previous) = ledger.previous_ledger.as_deref() {
      persist_managed_target_binding_ledger(profile_manager, &profile, previous)?;
      restore_managed_marker_metadata(profile_manager, &profile, previous)?;
    } else {
      remove_managed_target_binding_ledger(profile_manager, &profile)?;
      clear_managed_marker_metadata(profile_manager, &profile)?;
    }
    if receipt_matches {
      remove_chromium_launch_receipt(profile_manager, &profile)?;
    }
    log::warn!(
      "UNRECOVERABLE_BINDING_SESSION_RECONCILED profile={} action={} process_owned={}",
      profile.id,
      if rollback {
        "ROLLBACK_AND_CLEAR"
      } else {
        "CLEAR_WITHOUT_ROLLBACK"
      },
      identity_matches
    );
  }
  Ok(())
}

/// One-shot, fail-closed migration for profiles created before managed Grok
/// marker metadata existed. The exact target identity is supplied only to the
/// local runtime process and must match the persisted profile identity and the
/// live CDP target before any write occurs.
pub async fn migrate_managed_grok_target_on_startup(
) -> Result<Option<StartupGrokMigrationResult>, String> {
  let Some(hint) = startup_migration_hint_from_env()? else {
    return Ok(None);
  };
  let profile_manager = ProfileManager::instance();
  let mut profile = profile_manager
    .list_profiles()
    .map_err(|error| error.to_string())?
    .into_iter()
    .find(|profile| profile.id == hint.profile_id)
    .ok_or("MANAGED_GROK_MIGRATION_PROFILE_NOT_FOUND")?;
  validate_startup_migration_identity(&profile, &hint)?;

  let profiles_dir = profile_manager.get_profiles_dir();
  let cdp_port_path = crate::ephemeral_dirs::get_effective_profile_path(&profile, &profiles_dir)
    .join("floword-chromium")
    .join(".floword-cdp-port");
  let persisted_port = std::fs::read_to_string(cdp_port_path)
    .map_err(|_| "MANAGED_GROK_MIGRATION_CDP_PORT_MISSING".to_string())?
    .trim()
    .parse::<u16>()
    .map_err(|_| "MANAGED_GROK_MIGRATION_CDP_PORT_INVALID".to_string())?;
  if persisted_port != hint.cdp_port {
    return Err("MANAGED_GROK_MIGRATION_CDP_PORT_MISMATCH".into());
  }

  let (pages_before, snapshot_count, stabilization_elapsed_ms) =
    stabilized_cdp_pages(hint.cdp_port, Duration::from_secs(5))
      .await
      .map_err(|error| {
        if error == "GROK_TARGET_SET_UNSTABLE" {
          "GROK_TARGET_RECONCILING".to_string()
        } else {
          error
        }
      })?;
  let refreshed_profile = profile_manager
    .list_profiles()
    .map_err(|error| error.to_string())?
    .into_iter()
    .find(|candidate| candidate.id == hint.profile_id)
    .ok_or("MANAGED_GROK_MIGRATION_PROFILE_NOT_FOUND")?;
  validate_startup_migration_identity(&refreshed_profile, &hint)?;
  profile = refreshed_profile;
  let before_ids = pages_before
    .iter()
    .map(|page| page.id.as_str())
    .collect::<std::collections::BTreeSet<_>>();

  let (marker_id, marker_created) = startup_migration_marker_id(&profile);
  let mut markers = HashMap::new();
  for target in pages_before
    .iter()
    .filter(|target| is_exact_grok_page_url(&target.url))
  {
    let marker = read_marker(target)
      .await
      .map_err(|_| "GROK_TARGET_MARKER_CONFLICT".to_string())?;
    markers.insert(target.id.clone(), marker);
  }
  let (target, selection_path) =
    select_startup_migration_target(&profile, &hint, &pages_before, &markers, &marker_id)?;
  let transaction_id = uuid::Uuid::new_v4().to_string();
  let existing_marker = markers.get(&target.id).and_then(Option::as_ref);
  let marker = existing_marker
    .filter(|existing| marker_matches_current_identity(existing, &profile, &marker_id))
    .cloned()
    .unwrap_or_else(|| ManagedGrokMarker {
      version: 1,
      marker_id: marker_id.clone(),
      profile_id: profile.id.to_string(),
      browser_pid: hint.browser_pid,
      launch_generation: hint.launch_generation,
      transaction_id: transaction_id.clone(),
    });
  let already_marked = existing_marker == Some(&marker);

  let stabilization_started_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs();
  let mut ledger = GrokLaunchLedger {
    launch_transaction_id: transaction_id.clone(),
    profile_id: profile.id.to_string(),
    browser_pid: profile.process_id,
    launch_generation: profile.last_launch,
    target_stabilization_started_at: stabilization_started_at,
    preexisting_page_target_count: pages_before.len(),
    preexisting_grok_target_count: pages_before
      .iter()
      .filter(|page| is_exact_grok_page_url(&page.url))
      .count(),
    preexisting_target_id_hashes: pages_before
      .iter()
      .map(|page| target_id_hash(&page.id))
      .collect(),
    marker_matched_target_count: markers
      .values()
      .flatten()
      .filter(|existing| marker_belongs_to_profile(existing, &profile, &marker_id))
      .count(),
    created_target_count: 0,
    created_target_id_hashes: Vec::new(),
    navigated_blank_target_count: 0,
    selected_target_id: Some(target_id_hash(&target.id)),
    selection_path: Some(selection_path.to_string()),
    closed_target_count: 0,
    final_grok_target_count: pages_before
      .iter()
      .filter(|page| is_exact_grok_page_url(&page.url))
      .count(),
    snapshot_count,
    stabilization_elapsed_ms,
  };
  persist_grok_ledger(profile_manager, &profile, &ledger, "MIGRATION_PRE_WRITE")?;

  let fragment_matches =
    read_marker_binding_id(&target).await?.as_deref() == Some(marker_id.as_str());
  if (!already_marked || !fragment_matches) && !write_marker(&target, &marker).await? {
    return Err("MANAGED_GROK_MARKER_WRITE_FAILED".into());
  }
  if read_marker(&target).await?.as_ref() != Some(&marker)
    || read_marker_binding_id(&target).await?.as_deref() != Some(marker_id.as_str())
  {
    return Err("MANAGED_GROK_MARKER_VERIFY_FAILED".into());
  }

  let (pages_after, _, _) = stabilized_cdp_pages(hint.cdp_port, Duration::from_secs(5))
    .await
    .map_err(|error| {
      if error == "GROK_TARGET_SET_UNSTABLE" {
        "GROK_TARGET_RECONCILING".to_string()
      } else {
        error
      }
    })?;
  let refreshed_profile = profile_manager
    .list_profiles()
    .map_err(|error| error.to_string())?
    .into_iter()
    .find(|candidate| candidate.id == hint.profile_id)
    .ok_or("MANAGED_GROK_MIGRATION_PROFILE_NOT_FOUND")?;
  validate_startup_migration_identity(&refreshed_profile, &hint)?;
  let after_ids = pages_after
    .iter()
    .map(|page| page.id.as_str())
    .collect::<std::collections::BTreeSet<_>>();
  if before_ids != after_ids {
    return Err("MANAGED_GROK_MIGRATION_TARGET_SET_CHANGED".into());
  }

  let created_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs();
  profile.managed_grok_marker_version = Some(1);
  profile.managed_grok_marker_id = Some(marker_id);
  profile
    .managed_grok_marker_created_at
    .get_or_insert(created_at);
  profile.managed_grok_target_id = Some(target.id.clone());
  profile.managed_grok_browser_pid = Some(hint.browser_pid);
  profile.managed_grok_cdp_port = Some(hint.cdp_port);
  profile.managed_grok_launch_generation = Some(hint.launch_generation);
  profile_manager
    .save_profile(&profile)
    .map_err(|error| error.to_string())?;

  ledger.final_grok_target_count = pages_after
    .iter()
    .filter(|page| is_exact_grok_page_url(&page.url))
    .count();
  persist_grok_ledger(profile_manager, &profile, &ledger, "MIGRATION_COMMITTED")?;

  let result = StartupGrokMigrationResult {
    profile_id: profile.id.to_string(),
    target_id_hash: target_id_hash(&target.id),
    marker_written: marker_created || !already_marked,
    marker_verified: true,
    target_count_before: pages_before.len(),
    target_count_after: pages_after.len(),
    selection_path: selection_path.to_string(),
    created_target_count: 0,
    closed_target_count: 0,
    navigated_target_count: 0,
    reloaded_target_count: 0,
  };
  log::info!(
    "MANAGED_GROK_MARKER_MIGRATED {}",
    serde_json::to_string(&result).map_err(|error| error.to_string())?
  );
  Ok(Some(result))
}

async fn ensure_grok_target(
  port: u16,
  timeout: Duration,
  profile: &mut BrowserProfile,
  profile_manager: &'static ProfileManager,
  allow_startup_blank_target: bool,
) -> Result<(CdpPageTarget, bool, String), String> {
  let transaction_id = uuid::Uuid::new_v4().to_string();
  let stabilization_started_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs();
  let (mut pages, snapshot_count, stabilization_elapsed_ms) =
    stabilized_cdp_pages(port, timeout).await?;
  let preexisting_grok_target_count = pages.iter().filter(|p| is_grok_url(&p.url)).count();
  let mut ledger = GrokLaunchLedger {
    launch_transaction_id: transaction_id.clone(),
    profile_id: profile.id.to_string(),
    browser_pid: profile.process_id,
    launch_generation: profile.last_launch,
    target_stabilization_started_at: stabilization_started_at,
    preexisting_page_target_count: pages.len(),
    preexisting_grok_target_count,
    preexisting_target_id_hashes: pages.iter().map(|p| target_id_hash(&p.id)).collect(),
    marker_matched_target_count: 0,
    created_target_count: 0,
    created_target_id_hashes: Vec::new(),
    navigated_blank_target_count: 0,
    selected_target_id: None,
    selection_path: None,
    closed_target_count: 0,
    final_grok_target_count: preexisting_grok_target_count,
    snapshot_count,
    stabilization_elapsed_ms,
  };
  if let Err(error) = persist_grok_ledger(profile_manager, profile, &ledger, "PRE_MUTATION") {
    log::warn!("Failed to persist Grok launch ledger before mutation: {error}");
  }
  log::info!(
    "Floword Grok launch ledger pre-mutation: {}",
    serde_json::to_string(&ledger).unwrap()
  );

  let marker_id = profile
    .managed_grok_marker_id
    .clone()
    .unwrap_or_else(new_opaque_marker_id);
  profile.managed_grok_marker_version = Some(1);
  profile.managed_grok_marker_id = Some(marker_id.clone());
  profile
    .managed_grok_marker_created_at
    .get_or_insert(stabilization_started_at);
  let binding_ledger = read_managed_target_binding_ledger(profile_manager, profile);
  let restore_marker = ManagedGrokMarker {
    version: 1,
    marker_id: marker_id.clone(),
    profile_id: profile.id.to_string(),
    browser_pid: profile.process_id.unwrap_or_default(),
    launch_generation: profile.last_launch.unwrap_or_default(),
    transaction_id: transaction_id.clone(),
  };
  if let Some(first_page) = pages.first() {
    capture_marker_checkpoint(
      profile_manager,
      profile,
      first_page,
      &restore_marker.marker_id,
      profile.last_launch,
      "RESTORE_FIRST_TARGET_SNAPSHOT",
      tokio::time::Instant::now(),
    )
    .await;
  }
  for page in pages.iter().filter(|page| is_grok_url(&page.url)) {
    capture_marker_checkpoint(
      profile_manager,
      profile,
      page,
      &restore_marker.marker_id,
      profile.last_launch,
      "RESTORE_FIRST_PAGE_INSPECTION",
      tokio::time::Instant::now(),
    )
    .await;
  }

  let exact_generation_mapping = profile.managed_grok_browser_pid == profile.process_id
    && profile.managed_grok_cdp_port == Some(port)
    && profile.managed_grok_launch_generation == profile.last_launch;
  let mut selected = exact_generation_mapping
    .then_some(profile.managed_grok_target_id.as_ref())
    .flatten()
    .and_then(|id| {
      pages
        .iter()
        .find(|p| &p.id == id && is_grok_url(&p.url))
        .cloned()
    });
  if selected.is_some() {
    ledger.selection_path = Some("EXACT_CURRENT_GENERATION_MAPPING".into());
  }

  if selected.is_none() {
    let mut marker_pages = Vec::new();
    let mut conflicting_marker = false;
    let mut stale_marker = false;
    for page in pages.iter().filter(|p| is_grok_url(&p.url)) {
      let marker = read_marker(page).await;
      let fragment_marker_id = read_marker_binding_id(page)
        .await
        .unwrap_or_else(|_| marker_id_from_fragment(&page.url));
      match marker {
        Ok(Some(marker))
          if marker_matches_binding_ledger(&marker, profile, binding_ledger.as_ref())
            || fragment_marker_id.as_deref()
              == binding_ledger
                .as_ref()
                .filter(|ledger| ledger.lifecycle == "COMMITTED")
                .map(|ledger| ledger.managed_target_binding_id.as_str()) =>
        {
          marker_pages.push(page.clone());
        }
        Ok(Some(marker)) if marker_belongs_to_profile(&marker, profile, &marker_id) => {
          stale_marker = true;
        }
        Ok(Some(_)) | Err(_) => conflicting_marker = true,
        Ok(None)
          if fragment_marker_id.as_deref()
            == binding_ledger
              .as_ref()
              .filter(|ledger| ledger.lifecycle == "COMMITTED")
              .map(|ledger| ledger.managed_target_binding_id.as_str()) =>
        {
          marker_pages.push(page.clone());
        }
        Ok(None) => {}
      }
    }
    ledger.marker_matched_target_count = marker_pages.len();
    match marker_pages.len() {
      1 => {
        selected = marker_pages.into_iter().next();
        ledger.selection_path = Some("DURABLE_MARKER".into());
      }
      n if n > 1 => {
        ledger.selection_path = Some("MARKER_CONFLICT".into());
        let _ = persist_grok_ledger(profile_manager, profile, &ledger, "CONFLICT");
        log::warn!(
          "Floword Grok launch ledger: {}",
          serde_json::to_string(&ledger).unwrap()
        );
        return Err("MARKER_CONFLICT".into());
      }
      _ => {}
    }
    if selected.is_none() && conflicting_marker {
      ledger.selection_path = Some("MARKER_CONFLICT".into());
      let _ = persist_grok_ledger(profile_manager, profile, &ledger, "CONFLICT");
      return Err("GROK_TARGET_MARKER_CONFLICT".into());
    }
    if selected.is_none() && stale_marker {
      ledger.selection_path = Some("STALE_MANAGED_TARGET_MARKER".into());
      let _ = persist_grok_ledger(profile_manager, profile, &ledger, "STALE_MARKER");
      return Err("STALE_MANAGED_TARGET_MARKER".into());
    }
    if selected.is_none()
      && !allow_startup_blank_target
      && binding_ledger
        .as_ref()
        .is_some_and(|ledger| ledger.lifecycle == "COMMITTED")
    {
      ledger.selection_path = Some("DURABLE_MARKER_MISSING".into());
      let _ = persist_grok_ledger(profile_manager, profile, &ledger, "STALE_MARKER");
      return Err("STALE_MANAGED_TARGET_MARKER".into());
    }
  }

  let mut preferred_target_id = selected.as_ref().map(|target| target.id.clone());
  if selected.is_none()
    && (profile.managed_grok_target_id.is_some()
      || profile.managed_grok_browser_pid != profile.process_id
      || profile.managed_grok_launch_generation != profile.last_launch)
  {
    ledger.selection_path = Some("STALE_PREVIOUS_GENERATION".into());
  }
  if selected.is_none() {
    let grok_pages = pages
      .iter()
      .filter(|p| is_grok_url(&p.url))
      .cloned()
      .collect::<Vec<_>>();
    if grok_pages.len() == 1 {
      selected = grok_pages.first().cloned();
      ledger.selection_path = Some("ADOPTED_SINGLE_EXISTING".into());
    } else if grok_pages.len() > 1 {
      ledger.selection_path = Some("AMBIGUOUS_MULTIPLE_UNMARKED".into());
      let _ = persist_grok_ledger(profile_manager, profile, &ledger, "AMBIGUOUS");
      log::warn!(
        "Floword Grok launch ledger: {}",
        serde_json::to_string(&ledger).unwrap()
      );
      let candidate_target_id_hashes = grok_pages
        .iter()
        .filter(|page| is_grok_url(&page.url))
        .map(|page| target_id_hash(&page.id))
        .collect::<Vec<_>>();
      return Err(
        serde_json::json!({
          "code": "AMBIGUOUS_GROK_TAB",
          "message": "multiple Grok page targets have no authoritative mapping",
          "details": {
            "grokCandidateCount": grok_pages.len(),
            "candidateTargetIdHashes": candidate_target_id_hashes,
            "authoritativeMappingPresent": exact_generation_mapping,
            "currentLaunchTargetPresent": false,
            "selectionPath": "AMBIGUOUS_DISCOVERY"
          }
        })
        .to_string(),
      );
    }
  }

  if selected.is_none() {
    let blanks = pages
      .iter()
      .filter(|p| is_blank_url(&p.url))
      .collect::<Vec<_>>();
    if !allow_startup_blank_target {
      let _ = persist_grok_ledger(
        profile_manager,
        profile,
        &ledger,
        "NO_ELIGIBLE_STARTUP_TARGET",
      );
      return Err("GROK_TAB_NOT_FOUND".into());
    }
    if blanks.len() == 1 {
      navigate_cdp_target(blanks[0], "https://grok.com/imagine").await?;
      preferred_target_id = Some(blanks[0].id.clone());
      ledger.navigated_blank_target_count = 1;
      ledger.selection_path = Some("ADOPTED_SINGLE_STARTUP_BLANK".into());
    } else if pages.is_empty() {
      let value: serde_json::Value = reqwest::Client::new()
        .put(format!(
          "http://127.0.0.1:{port}/json/new?https://grok.com/imagine"
        ))
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
      let target = CdpPageTarget {
        id: value
          .get("id")
          .and_then(|v| v.as_str())
          .ok_or("GROK_TARGET_CREATE_FAILED")?
          .to_string(),
        url: value
          .get("url")
          .and_then(|v| v.as_str())
          .unwrap_or("https://grok.com/imagine")
          .to_string(),
        websocket: value
          .get("webSocketDebuggerUrl")
          .and_then(|v| v.as_str())
          .ok_or("GROK_TARGET_CREATE_FAILED")?
          .to_string(),
        title: value
          .get("title")
          .and_then(|v| v.as_str())
          .unwrap_or_default()
          .to_string(),
      };
      ledger.created_target_count = 1;
      ledger
        .created_target_id_hashes
        .push(target_id_hash(&target.id));
      ledger.selection_path = Some("CREATED_WHEN_NO_PAGE".into());
      preferred_target_id = Some(target.id.clone());
      selected = Some(target);
    }
  }

  let deadline = tokio::time::Instant::now() + timeout;
  let navigation_target_id = ledger
    .navigated_blank_target_count
    .eq(&1)
    .then(|| preferred_target_id.clone())
    .flatten();
  if let Some(target_id) = navigation_target_id.as_deref() {
    selected = Some(wait_for_navigated_grok_target(port, target_id, deadline).await?);
  }
  while selected.is_none() && tokio::time::Instant::now() < deadline {
    pages = list_cdp_pages(port).await.unwrap_or_default();
    selected = preferred_target_id.as_ref().and_then(|id| {
      pages
        .iter()
        .find(|p| &p.id == id && is_grok_url(&p.url))
        .cloned()
    });
    tokio::time::sleep(Duration::from_millis(250)).await;
  }
  let mut selected = selected.ok_or("GROK_COLD_START_NAVIGATION_FAILED")?;
  if !is_grok_url(&selected.url) {
    pages = list_cdp_pages(port).await.unwrap_or_default();
    selected = pages
      .into_iter()
      .find(|p| p.id == selected.id && is_grok_url(&p.url))
      .ok_or("GROK_COLD_START_NAVIGATION_FAILED")?;
  }
  let marker = ManagedGrokMarker {
    version: 1,
    marker_id: marker_id.clone(),
    profile_id: profile.id.to_string(),
    browser_pid: profile.process_id.ok_or("GROK_BROWSER_IDENTITY_CHANGED")?,
    launch_generation: profile.last_launch.ok_or("GROK_BROWSER_IDENTITY_CHANGED")?,
    transaction_id,
  };
  capture_marker_checkpoint(
    profile_manager,
    profile,
    &selected,
    &marker.marker_id,
    Some(marker.launch_generation),
    "RESTORE_SELECTION",
    tokio::time::Instant::now(),
  )
  .await;
  if !write_marker(&selected, &marker).await? {
    return Err("MANAGED_GROK_MARKER_WRITE_FAILED".into());
  }
  let previous_profile = profile.clone();
  profile.managed_grok_target_id = Some(selected.id.clone());
  profile.managed_grok_browser_pid = profile.process_id;
  profile.managed_grok_cdp_port = Some(port);
  profile.managed_grok_launch_generation = profile.last_launch;
  profile_manager
    .save_profile(profile)
    .map_err(|e| e.to_string())?;
  let now = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default()
    .as_secs();
  let committed_binding = ManagedTargetBindingLedger {
    profile_id: profile.id.to_string(),
    managed_target_binding_id: marker_id.clone(),
    last_known_target_id: selected.id.clone(),
    browser_pid: marker.browser_pid,
    cdp_port: port,
    launch_generation: marker.launch_generation,
    managed_grok_page_url: normalized_public_grok_url(&selected.url),
    binding_created_at: now,
    binding_version: 1,
    lifecycle: "COMMITTED".into(),
    executable: crate::browser::ChromiumBrowser::resolve_executable()
      .map(|path| path.to_string_lossy().to_string())
      .unwrap_or_default(),
    user_data_dir: crate::ephemeral_dirs::get_effective_profile_path(
      profile,
      &profile_manager.get_profiles_dir(),
    )
    .join("floword-chromium")
    .to_string_lossy()
    .to_string(),
    expires_at: None,
    candidates: vec![],
    previous_ledger: None,
  };
  if let Err(error) =
    persist_managed_target_binding_ledger(profile_manager, profile, &committed_binding)
  {
    let _ = clear_marker(&selected).await;
    let _ = profile_manager.save_profile(&previous_profile);
    return Err(error);
  }
  ledger.selected_target_id = Some(target_id_hash(&selected.id));
  ledger.final_grok_target_count = list_cdp_pages(port)
    .await
    .unwrap_or_default()
    .iter()
    .filter(|p| is_grok_url(&p.url))
    .count();
  if let Err(error) = persist_grok_ledger(profile_manager, profile, &ledger, "POST_MUTATION") {
    log::warn!("Failed to persist Grok launch ledger after mutation: {error}");
  }
  log::info!(
    "Floword Grok launch ledger post-mutation: {}",
    serde_json::to_string(&ledger).unwrap()
  );
  Ok((
    selected,
    ledger.created_target_count == 0,
    ledger.selection_path.unwrap_or_else(|| "UNKNOWN".into()),
  ))
}

async fn launch_with_url_policy<R, Check, CheckFuture, Launch, LaunchFuture>(
  profile_id: &str,
  policy: LaunchUrlPolicy,
  check_running: Check,
  launch: Launch,
) -> Result<(R, bool), String>
where
  Check: FnOnce() -> CheckFuture,
  CheckFuture: Future<Output = Result<bool, String>>,
  Launch: FnOnce(Option<String>) -> LaunchFuture,
  LaunchFuture: Future<Output = Result<R, String>>,
{
  let _profile_launch_guard = lock_profile_launch(profile_id).await;
  let (url, reused) = match policy {
    LaunchUrlPolicy::AlwaysOpen(url) => {
      // Preserve the generic API's URL-opening behavior while reporting
      // whether this invocation reused an already-running browser.
      (url, check_running().await?)
    }
    LaunchUrlPolicy::ColdStartOnly(url) => {
      if check_running().await? {
        (None, true)
      } else {
        (Some(url), false)
      }
    }
  };
  Ok((launch(url).await?, reused))
}

pub struct BrowserRunner {
  pub profile_manager: &'static ProfileManager,
  pub downloaded_browsers_registry: &'static DownloadedBrowsersRegistry,
  auto_updater: &'static crate::auto_updater::AutoUpdater,
  wayfern_manager: &'static WayfernManager,
}

impl BrowserRunner {
  fn new() -> Self {
    Self {
      profile_manager: ProfileManager::instance(),
      downloaded_browsers_registry: DownloadedBrowsersRegistry::instance(),
      auto_updater: crate::auto_updater::AutoUpdater::instance(),
      wayfern_manager: WayfernManager::instance(),
    }
  }

  pub fn instance() -> &'static BrowserRunner {
    &BROWSER_RUNNER
  }

  pub fn get_binaries_dir(&self) -> PathBuf {
    crate::app_dirs::binaries_dir()
  }

  /// Resolve the DNS blocklist level to a cached file path plus whether that
  /// file should be treated as an allowlist. If a level is set but the cache
  /// is missing, fetches/compiles on demand (blocks until done).
  async fn resolve_blocklist_file(
    profile: &crate::profile::BrowserProfile,
  ) -> Result<(Option<String>, bool), String> {
    let Some(ref level_str) = profile.dns_blocklist else {
      return Ok((None, false));
    };
    let Some(level) = crate::dns_blocklist::BlocklistLevel::parse_level(level_str) else {
      return Ok((None, false));
    };
    if level == crate::dns_blocklist::BlocklistLevel::None {
      return Ok((None, false));
    }
    // Only the user's custom list can be an allowlist; the Hagezi tiers are
    // always block lists.
    let allowlist_mode = level == crate::dns_blocklist::BlocklistLevel::Custom
      && crate::dns_blocklist::CustomDnsConfig::load().allowlist_mode;
    let path = crate::dns_blocklist::BlocklistManager::ensure_cached(level)
      .await
      .map_err(|e| format!("Failed to fetch DNS blocklist: {e}"))?;
    Ok((Some(path.to_string_lossy().to_string()), allowlist_mode))
  }

  /// Refresh cloud proxy credentials if the profile uses a cloud or cloud-derived proxy,
  /// then resolve the proxy settings with profile-specific sid for sticky sessions.
  async fn resolve_proxy_with_refresh(
    &self,
    proxy_id: Option<&String>,
    profile_id: Option<&str>,
  ) -> Result<Option<ProxySettings>, String> {
    let proxy_id = match proxy_id {
      Some(id) => id,
      None => return Ok(None),
    };

    if PROXY_MANAGER.is_cloud_or_derived(proxy_id) {
      log::info!("Refreshing cloud proxy credentials before launch for proxy {proxy_id}");
      CLOUD_AUTH.sync_cloud_proxy().await;
    }
    // For cloud-derived proxies, inject profile-specific sid for sticky sessions
    if let Some(pid) = profile_id {
      if PROXY_MANAGER.is_cloud_or_derived(proxy_id) {
        return Ok(PROXY_MANAGER.resolve_proxy_for_profile(proxy_id, pid));
      }
    }
    Ok(PROXY_MANAGER.get_proxy_settings_by_id(proxy_id))
  }

  fn fire_launch_hook(profile: &BrowserProfile) {
    let Some(raw_url) = profile.launch_hook.as_deref() else {
      return;
    };
    let trimmed = raw_url.trim();
    if trimmed.is_empty() {
      return;
    }

    let parsed = match url::Url::parse(trimmed) {
      Ok(u) => u,
      Err(e) => {
        log::warn!(
          "Skipping launch hook for profile {} (ID: {}): invalid URL: {e}",
          profile.name,
          profile.id
        );
        return;
      }
    };

    if !matches!(parsed.scheme(), "http" | "https") {
      log::warn!(
        "Skipping launch hook for profile {} (ID: {}): URL must be http or https",
        profile.name,
        profile.id
      );
      return;
    }

    let url = parsed.to_string();
    let url_label = crate::log_redaction::url_label(&url);

    log::info!("Firing launch hook GET {url_label}");

    tokio::spawn(async move {
      let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
      {
        Ok(c) => c,
        Err(e) => {
          log::warn!(
            "Launch hook client build failed: {}",
            crate::log_redaction::text(&e.to_string())
          );
          return;
        }
      };

      match client.get(&url).send().await {
        Ok(resp) => {
          log::info!("Launch hook {url_label} returned status {}", resp.status());
        }
        Err(e) => {
          log::warn!(
            "Launch hook {url_label} failed: {}",
            crate::log_redaction::text(&e.to_string())
          );
        }
      }
    });
  }

  async fn resolve_launch_proxy(
    &self,
    profile: &BrowserProfile,
  ) -> Result<Option<ProxySettings>, String> {
    Self::fire_launch_hook(profile);

    self
      .resolve_proxy_with_refresh(profile.proxy_id.as_ref(), Some(&profile.id.to_string()))
      .await
  }

  async fn launch_chromium_internal(
    &self,
    _app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: Option<String>,
    remote_debugging_port: Option<u16>,
    headless: bool,
  ) -> Result<BrowserProfile, Box<dyn std::error::Error + Send + Sync>> {
    let executable = crate::browser::ChromiumBrowser::resolve_executable()
      .map_err(|error| std::io::Error::other(error.to_string()))?;
    let profiles_dir = self.profile_manager.get_profiles_dir();
    let profile_path = crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
      .join("floword-chromium");
    std::fs::create_dir_all(&profile_path)?;
    let proxy = self
      .resolve_launch_proxy(profile)
      .await
      .map_err(std::io::Error::other)?;
    let mut args = crate::browser::ChromiumBrowser::new()
      .create_launch_args(
        &profile_path.to_string_lossy(),
        proxy.as_ref(),
        url,
        remote_debugging_port,
        headless,
      )
      .map_err(|error| std::io::Error::other(error.to_string()))?;
    let mut extension_paths: Vec<String> = std::env::var("FLOWORD_CHROMEX_EXTENSION_PATH")
      .ok()
      .filter(|path| std::path::Path::new(path).join("manifest.json").is_file())
      .map(|path| vec![path])
      .unwrap_or_default();
    if extension_paths.is_empty() {
      let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
      extension_paths = mgr
        .install_extensions_for_profile(profile, &profile_path)
        .unwrap_or_default();
    }
    if !extension_paths.is_empty() {
      let separator = if cfg!(windows) { ";" } else { "," };
      args.push(format!(
        "--load-extension={}",
        extension_paths
          .iter()
          .map(|p| p.to_string())
          .collect::<Vec<_>>()
          .join(separator)
      ));
      args.push(format!(
        "--disable-extensions-except={}",
        extension_paths
          .iter()
          .map(String::as_str)
          .collect::<Vec<_>>()
          .join(separator)
      ));
    }
    let mut command = std::process::Command::new(&executable);
    command.args(&args);
    let child = command
      .spawn()
      .map_err(|error| std::io::Error::other(format!("FLOWORD_CHROMIUM_LAUNCH_FAILED: {error}")))?;
    let launch_generation = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs();
    let launch_receipt = ChromiumLaunchReceipt {
      profile_id: profile.id.to_string(),
      browser_pid: child.id(),
      cdp_port: remote_debugging_port,
      launch_generation,
      executable: executable.to_string_lossy().to_string(),
      user_data_dir: profile_path.to_string_lossy().to_string(),
      spawned_at: launch_generation,
    };
    // This is intentionally the first persisted side effect after spawn.  It
    // lets the API report processSpawned=true and roll back the exact child if
    // CDP readiness or profile persistence fails immediately afterwards.
    persist_chromium_launch_receipt(self.profile_manager, profile, &launch_receipt)
      .map_err(std::io::Error::other)?;
    if let Some(port) = remote_debugging_port {
      std::fs::write(profile_path.join(".floword-cdp-port"), port.to_string())?;
    }
    let mut updated = profile.clone();
    updated.process_id = Some(child.id());
    // Persist the engine actually owning this profile so subsequent
    // ColdStartOnly requests can distinguish the dedicated Chromium process
    // from a legacy Wayfern process that may still be alive.
    updated.browser = "chromium".to_string();
    updated.last_launch = Some(launch_generation);
    updated.ephemeral = false;
    updated.clear_on_close = false;
    self.save_process_info(&updated)?;
    log::info!(
      "Floword Chromium launched for profile {} with PID {}",
      updated.id,
      child.id()
    );
    Ok(updated)
  }

  /// Get the executable path for a browser profile
  /// This is a common helper to eliminate code duplication across the codebase
  pub fn get_browser_executable_path(
    &self,
    profile: &BrowserProfile,
  ) -> Result<PathBuf, Box<dyn std::error::Error + Send + Sync>> {
    // Create browser instance to get executable path
    let browser_type = crate::browser::BrowserType::from_str(&profile.browser)
      .map_err(|e| format!("Invalid browser type: {e}"))?;
    let browser = crate::browser::create_browser(browser_type);

    // Construct browser directory path: binaries/<browser>/<version>/
    let mut browser_dir = self.get_binaries_dir();
    browser_dir.push(&profile.browser);
    browser_dir.push(&profile.version);

    // Get platform-specific executable path
    browser
      .get_executable_path(&browser_dir)
      .map_err(|e| format!("Failed to get executable path for {}: {e}", profile.browser).into())
  }

  #[allow(clippy::too_many_arguments)]
  async fn launch_browser_internal(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: Option<String>,
    _local_proxy_settings: Option<&ProxySettings>,
    remote_debugging_port: Option<u16>,
    headless: bool,
    engine: crate::browser::BrowserType,
  ) -> Result<BrowserProfile, Box<dyn std::error::Error + Send + Sync>> {
    // Wayfern profiles are preserved as legacy data only.  The Local Free
    // product never launches or routes work through the retired engine.
    if profile.browser.eq_ignore_ascii_case("wayfern") {
      return Err(
        "LEGACY_UNSUPPORTED: Wayfern profiles are read-only; create a Chrome for Testing profile"
          .into(),
      );
    }
    if engine == crate::browser::BrowserType::Chromium {
      return self
        .launch_chromium_internal(app_handle, profile, url, remote_debugging_port, headless)
        .await;
    }
    // Handle Wayfern profiles using WayfernManager
    if profile.browser == "wayfern" {
      // Get or create wayfern config
      let mut wayfern_config = profile.wayfern_config.clone().unwrap_or_else(|| {
        log::info!(
          "No wayfern config found for profile {}, using default",
          profile.name
        );
        WayfernConfig::default()
      });

      // Always start a local proxy for Wayfern (for traffic monitoring and geoip support)
      let mut upstream_proxy = self
        .resolve_launch_proxy(profile)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
      let geo_proxy_signature_settings = upstream_proxy.clone();

      struct XrayLaunchGuard {
        worker_id: Option<String>,
        profile_name: String,
      }
      impl Drop for XrayLaunchGuard {
        fn drop(&mut self) {
          let Some(worker_id) = self.worker_id.take() else {
            return;
          };
          log::warn!(
            "Launch failed after Xray-core start for profile {}; stopping worker",
            self.profile_name
          );
          if let Err(error) = crate::xray_worker_runner::stop_xray_worker_now(&worker_id) {
            log::warn!("Failed to stop Xray-core worker after failed launch: {error}");
          }
        }
      }
      let mut xray_launch_guard = XrayLaunchGuard {
        worker_id: None,
        profile_name: profile.name.clone(),
      };
      struct PoolLeaseLaunchGuard {
        profile_id: String,
        armed: bool,
      }
      impl Drop for PoolLeaseLaunchGuard {
        fn drop(&mut self) {
          if self.armed {
            let profile_id = self.profile_id.clone();
            tauri::async_runtime::spawn(async move {
              let _ = crate::vpn::pool::release_profile_lease(&profile_id).await;
            });
          }
        }
      }
      let mut pool_lease_launch_guard = PoolLeaseLaunchGuard {
        profile_id: profile.id.to_string(),
        armed: false,
      };

      if upstream_proxy
        .as_ref()
        .is_some_and(|proxy| proxy.proxy_type.eq_ignore_ascii_case("vless"))
      {
        let vless_uri = upstream_proxy
          .as_ref()
          .and_then(|proxy| proxy.vless_uri.as_deref())
          .ok_or_else(|| crate::backend_error("VLESS_CONFIG_INVALID"))?;
        let worker =
          crate::xray_worker_runner::start_xray_worker(Some(&profile.id.to_string()), vless_uri)
            .await
            .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> {
              error.to_string().into()
            })?;
        log::info!(
          "Xray-core worker started for Wayfern profile on port {}",
          worker.local_port
        );
        xray_launch_guard.worker_id = Some(worker.id.clone());
        upstream_proxy = Some(worker.local_proxy_settings());
      }

      // If profile has a VPN instead of proxy, start VPN worker and use it as upstream
      if upstream_proxy.is_none() {
        if let Some(ref vpn_id) = profile.vpn_id {
          let pool_id = crate::vpn::pool::parse_pool_reference(vpn_id);
          let pool_lease = match pool_id {
            Some(pool_id) => Some(
              crate::vpn::pool::acquire_profile_lease(pool_id, &profile.id.to_string())
                .await
                .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?,
            ),
            None => None,
          };
          pool_lease_launch_guard.armed = pool_lease.is_some();
          let worker_result = if let Some(lease) = &pool_lease {
            crate::vpn_worker_storage::find_vpn_worker_by_vpn_id(&lease.config_id)
              .ok_or_else(|| "VPN pool lease worker is unavailable".to_string())
          } else {
            crate::vpn_worker_runner::start_vpn_worker(vpn_id)
              .await
              .map_err(|error| error.to_string())
          };
          match worker_result {
            Ok(vpn_worker) => {
              if let Some(port) = vpn_worker.local_port {
                upstream_proxy = Some(ProxySettings {
                  proxy_type: "socks5".to_string(),
                  host: "127.0.0.1".to_string(),
                  port,
                  username: None,
                  password: None,
                  vless_uri: None,
                });
                log::info!("VPN worker started for Wayfern profile on port {}", port);
              }
            }
            Err(e) => {
              if pool_lease.is_some() {
                let _ = crate::vpn::pool::release_profile_lease(&profile.id.to_string()).await;
              }
              return Err(format!("Failed to start VPN worker: {e}").into());
            }
          }
        }
      }

      log::info!(
        "Starting local proxy for Wayfern profile: {} (upstream: {})",
        profile.name,
        upstream_proxy
          .as_ref()
          .map(|p| format!("{}:{}", p.host, p.port))
          .unwrap_or_else(|| "DIRECT".to_string())
      );

      // Start the proxy and get local proxy settings
      // If proxy startup fails, DO NOT launch Wayfern - it requires local proxy
      let profile_id_str = profile.id.to_string();
      let (blocklist_file, dns_allowlist_mode) = Self::resolve_blocklist_file(profile).await?;
      // Unique per-launch key: a shared constant here would let concurrent
      // launches overwrite each other's active_proxies entry, ending with one
      // browser's worker tracked under another browser's PID.
      let launch_placeholder_pid = crate::proxy_manager::next_launch_placeholder_pid();
      let local_proxy = PROXY_MANAGER
        .start_proxy(
          app_handle.clone(),
          upstream_proxy.as_ref(),
          launch_placeholder_pid,
          Some(&profile_id_str),
          profile.proxy_bypass_rules.clone(),
          blocklist_file,
          dns_allowlist_mode,
          // Wayfern (Chromium) uses a local SOCKS5 proxy so QUIC and WebRTC
          // UDP can be routed through it (via SOCKS5 UDP ASSOCIATE) without
          // leaking the real IP, rather than being forced direct as they
          // would be over an HTTP CONNECT proxy.
          "socks5",
        )
        .await
        .map_err(|e| {
          let error_msg = crate::wrap_backend_error(e, "Failed to start local proxy for Wayfern");
          log::error!("{}", error_msg);
          error_msg
        })?;

      // If any step below fails before the browser is up, the detached worker
      // must be stopped here: its config never gets a browser_pid, so neither
      // the GUI sweeps nor the worker's own watchdog would ever reap it — it
      // would survive until machine reboot.
      struct ProxyLaunchGuard {
        app_handle: tauri::AppHandle,
        routing_pid: u32,
        profile_name: String,
        armed: bool,
      }
      impl Drop for ProxyLaunchGuard {
        fn drop(&mut self) {
          if self.armed {
            log::warn!(
              "Launch failed after local proxy start for profile {}; stopping proxy worker",
              self.profile_name
            );
            let app_handle = self.app_handle.clone();
            let pid = self.routing_pid;
            tauri::async_runtime::spawn(async move {
              if let Err(e) = PROXY_MANAGER.stop_proxy(app_handle, pid).await {
                log::warn!("Failed to stop proxy worker after failed launch: {e}");
              }
            });
          }
        }
      }
      let mut proxy_launch_guard = ProxyLaunchGuard {
        app_handle: app_handle.clone(),
        routing_pid: launch_placeholder_pid,
        profile_name: profile.name.clone(),
        armed: true,
      };

      // Format proxy URL for wayfern - use SOCKS5 for the local proxy so
      // Chromium proxies UDP (QUIC/WebRTC), not just TCP.
      let proxy_url = format!("socks5://{}:{}", local_proxy.host, local_proxy.port);

      // Set proxy in wayfern config
      wayfern_config.proxy = Some(proxy_url);

      log::info!(
        "Configured local proxy for Wayfern: {:?}",
        wayfern_config.proxy
      );

      // Check if we need to generate a new fingerprint on every launch
      let mut updated_profile = profile.clone();
      if wayfern_config.randomize_fingerprint_on_launch == Some(true) {
        log::info!(
          "Generating random fingerprint for Wayfern profile: {}",
          profile.name
        );

        // Create a config copy without the existing fingerprint to force generation of a new one
        let mut config_for_generation = wayfern_config.clone();
        config_for_generation.fingerprint = None;

        // Generate a new fingerprint
        let (new_fingerprint, geolocation_applied) = self
          .wayfern_manager
          .generate_fingerprint_config(&app_handle, profile, &config_for_generation)
          .await
          .map_err(|e| format!("Failed to generate random fingerprint: {e}"))?;

        log::info!(
          "New fingerprint generated, length: {} chars",
          new_fingerprint.len()
        );

        // Update the config with the new fingerprint for launching
        wayfern_config.fingerprint = Some(new_fingerprint.clone());

        // Save the updated fingerprint to the profile so it persists.
        let mut updated_wayfern_config = updated_profile.wayfern_config.clone().unwrap_or_default();
        updated_wayfern_config.fingerprint = Some(new_fingerprint);
        // Preserve the randomize flag so it persists across launches
        updated_wayfern_config.randomize_fingerprint_on_launch = Some(true);
        // Preserve the OS setting so it's used for future fingerprint generation
        if wayfern_config.os.is_some() {
          updated_wayfern_config.os = wayfern_config.os.clone();
        }
        // Record which routing this fresh fingerprint's geolocation was built
        // for (provenance only — nothing rewrites the fingerprint from it).
        // Only when geolocation actually applied; otherwise leave it unset so a
        // later on-demand match can tell the location was never resolved.
        updated_wayfern_config.geo_proxy_signature = if geolocation_applied {
          Some(crate::wayfern_manager::WayfernManager::geo_signature(
            geo_proxy_signature_settings.as_ref(),
            profile.vpn_id.as_deref(),
            wayfern_config.geoip.as_ref(),
          ))
        } else {
          None
        };
        updated_profile.wayfern_config = Some(updated_wayfern_config.clone());

        log::info!(
          "Updated profile wayfern_config with new fingerprint for profile: {}, fingerprint length: {}",
          profile.name,
          updated_wayfern_config.fingerprint.as_ref().map(|f| f.len()).unwrap_or(0)
        );
      }
      // A non-randomize profile keeps its configured fingerprint verbatim, even
      // when its proxy/VPN routing has changed since the fingerprint was built.
      // We deliberately do NOT silently rewrite its timezone/language to match
      // the new exit: that hid every real fingerprint-vs-exit mismatch (a US
      // fingerprint behind a German exit would be quietly relabelled German
      // before the launch-time consistency check could see it). The check now
      // surfaces the mismatch, and the user re-matches on demand via
      // `match_profile_fingerprint_to_exit`.

      // Always force persistent storage for user profiles so logins (Grok, ChatGPT, Social) are preserved permanently
      updated_profile.ephemeral = false;
      updated_profile.clear_on_close = false;

      // Create ephemeral dir only if password-protected
      if profile.password_protected {
        crate::profile::password::prepare_for_launch(profile)
          .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })?;
      }

      // Launch Wayfern browser
      log::info!("Launching Wayfern for profile: {}", profile.name);

      // Get profile path for Wayfern
      let profiles_dir = self.profile_manager.get_profiles_dir();
      let profile_data_path =
        crate::ephemeral_dirs::get_effective_profile_path(&updated_profile, &profiles_dir);
      let profile_path_str = profile_data_path.to_string_lossy().to_string();

      // Always called: app-bundled extensions load even when the profile has no
      // extension group assigned.
      let mut extension_paths = Vec::new();
      {
        let mgr = crate::extension_manager::EXTENSION_MANAGER.lock().unwrap();
        match mgr.install_extensions_for_profile(&updated_profile, &profile_data_path) {
          Ok(paths) => {
            if !paths.is_empty() {
              log::info!(
                "Prepared {} Chromium extensions for profile: {}",
                paths.len(),
                updated_profile.name
              );
            }
            extension_paths = paths;
          }
          Err(e) => {
            log::warn!("Failed to install extensions for Wayfern profile: {e}");
          }
        }
      }

      // Get proxy URL from config
      let proxy_url = wayfern_config.proxy.as_deref();

      let wayfern_result = self
        .wayfern_manager
        .launch_wayfern(
          &app_handle,
          &updated_profile,
          &profile_path_str,
          &wayfern_config,
          url.as_deref(),
          proxy_url,
          profile.ephemeral,
          &extension_paths,
          remote_debugging_port,
          headless,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
          format!("Failed to launch Wayfern: {e}").into()
        })?;

      // Get the process ID from launch result
      let Some(process_id) = wayfern_result.processId.filter(|pid| *pid != 0) else {
        if let Err(error) = self.wayfern_manager.stop_wayfern(&wayfern_result.id).await {
          log::warn!("Failed to stop Wayfern after it omitted its process ID: {error}");
        }
        return Err(
          crate::backend_error_with_detail(
            "INTERNAL_ERROR",
            "Wayfern did not report a process identifier",
          )
          .into(),
        );
      };
      log::info!("Wayfern launched successfully with PID: {process_id}");

      if let Err(error) = PROXY_MANAGER.update_proxy_pid(launch_placeholder_pid, process_id) {
        if let Err(stop_error) = self.wayfern_manager.stop_wayfern(&wayfern_result.id).await {
          log::warn!("Failed to stop Wayfern after proxy PID mapping failed: {stop_error}");
        }
        return Err(crate::backend_error_with_detail("INTERNAL_ERROR", error).into());
      }
      proxy_launch_guard.routing_pid = process_id;
      log::info!(
        "Updated proxy PID mapping from launch placeholder {launch_placeholder_pid} to actual PID: {process_id}"
      );
      if profile
        .vpn_id
        .as_deref()
        .and_then(crate::vpn::pool::parse_pool_reference)
        .is_some()
      {
        crate::vpn::pool::monitor_profile_lease(profile.id.to_string(), process_id);
        pool_lease_launch_guard.armed = false;
      }
      if let Err(error) =
        PROXY_MANAGER.set_browser_pid_for_profile(&updated_profile.id.to_string(), process_id)
      {
        if let Err(error) = self.wayfern_manager.stop_wayfern(&wayfern_result.id).await {
          log::warn!("Failed to stop Wayfern after proxy worker reassignment failed: {error}");
        }
        return Err(
          crate::backend_error_with_detail("PROXY_BROWSER_PID_BIND_FAILED", error).into(),
        );
      }
      if let Some(worker_id) = xray_launch_guard.worker_id.as_deref() {
        if !crate::xray_worker_runner::set_browser_pid(worker_id, process_id) {
          if let Err(error) = self.wayfern_manager.stop_wayfern(&wayfern_result.id).await {
            log::warn!("Failed to stop Wayfern after Xray worker reassignment failed: {error}");
          }
          return Err(crate::backend_error("XRAY_START_FAILED").into());
        }
      }

      // The browser and both detached routing workers now share one verified
      // process identity, so later profile-persistence failures must not tear
      // down a live route.
      proxy_launch_guard.armed = false;
      xray_launch_guard.worker_id = None;

      // Wayfern.setFingerprint echoes back the fingerprint the browser actually
      // applied, which may be UPGRADED from the stored one (e.g. when the
      // stored fingerprint targets an older browser version). Persist it so the
      // next launch starts from the upgraded value — saved below via
      // save_process_info(&updated_profile).
      if let Some(used_fp) = wayfern_result.used_fingerprint.clone() {
        let mut cfg = updated_profile.wayfern_config.clone().unwrap_or_default();
        if cfg.fingerprint.as_deref() != Some(used_fp.as_str()) {
          log::info!(
            "Persisting upgraded fingerprint from Wayfern.setFingerprint for profile: {} (len {})",
            profile.name,
            used_fp.len()
          );
          cfg.fingerprint = Some(used_fp);
          updated_profile.wayfern_config = Some(cfg);
        }
      }

      // Update profile with the process info
      updated_profile.process_id = Some(process_id);
      updated_profile.last_launch = Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs());

      // Save the updated profile
      log::info!(
        "Saving profile {} with wayfern_config fingerprint length: {}",
        updated_profile.name,
        updated_profile
          .wayfern_config
          .as_ref()
          .and_then(|c| c.fingerprint.as_ref())
          .map(|f| f.len())
          .unwrap_or(0)
      );
      self.save_process_info(&updated_profile)?;
      let _ = crate::tag_manager::TAG_MANAGER.lock().map(|tm| {
        let _ = tm.rebuild_from_profiles(&self.profile_manager.list_profiles().unwrap_or_default());
      });
      log::info!(
        "Successfully saved profile with process info: {}",
        updated_profile.name
      );

      // Emit profiles-changed to trigger frontend to reload profiles from disk
      if let Err(e) = events::emit_empty("profiles-changed") {
        log::warn!("Warning: Failed to emit profiles-changed event: {e}");
      }

      log::info!(
        "Emitting profile events for successful Wayfern launch: {}",
        updated_profile.name
      );

      // Emit profile update event to frontend
      if let Err(e) = events::emit("profile-updated", &updated_profile) {
        log::warn!("Warning: Failed to emit profile update event: {e}");
      }

      // Emit minimal running changed event to frontend
      #[derive(Serialize)]
      struct RunningChangedPayload {
        id: String,
        is_running: bool,
      }

      let payload = RunningChangedPayload {
        id: updated_profile.id.to_string(),
        is_running: updated_profile.process_id.is_some(),
      };

      if let Err(e) = events::emit("profile-running-changed", &payload) {
        log::warn!("Warning: Failed to emit profile running changed event: {e}");
      } else {
        log::info!(
          "Successfully emitted profile-running-changed event for Wayfern {}: running={}",
          updated_profile.name,
          payload.is_running
        );
      }

      return Ok(updated_profile);
    }

    Err(format!("Unsupported browser type: {}", profile.browser).into())
  }

  pub async fn open_url_in_existing_browser(
    &self,
    _app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: &str,
    _internal_proxy_settings: Option<&ProxySettings>,
  ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if profile.browser.eq_ignore_ascii_case("wayfern") {
      return Err(
        "LEGACY_UNSUPPORTED: Wayfern profiles are read-only; create a Chrome for Testing profile"
          .into(),
      );
    }
    // Handle Wayfern profiles using WayfernManager
    if profile.browser == "wayfern" {
      let profiles_dir = self.profile_manager.get_profiles_dir();
      let profile_data_path =
        crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir);
      let profile_path_str = profile_data_path.to_string_lossy();

      // Check if the process is running
      match self
        .wayfern_manager
        .find_wayfern_by_profile(&profile_path_str)
        .await
      {
        Some(_wayfern_process) => {
          log::info!(
            "Opening URL in existing Wayfern process for profile: {} (ID: {})",
            profile.name,
            profile.id
          );

          // Use CDP to open URL in a new tab
          self
            .wayfern_manager
            .open_url_in_tab(&profile_path_str, url)
            .await?;
          return Ok(());
        }
        None => {
          return Err("Wayfern browser is not running".into());
        }
      }
    }

    Err(format!("Unsupported browser type: {}", profile.browser).into())
  }

  pub async fn launch_browser_with_debugging(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: Option<String>,
    remote_debugging_port: Option<u16>,
    headless: bool,
    engine: crate::browser::BrowserType,
  ) -> Result<BrowserProfile, Box<dyn std::error::Error + Send + Sync>> {
    self
      .launch_browser_with_debugging_intent(
        app_handle,
        profile,
        url,
        remote_debugging_port,
        headless,
        engine,
        ManagedLaunchIntent::Run,
      )
      .await
  }

  #[allow(clippy::too_many_arguments)]
  async fn launch_browser_with_debugging_intent(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: Option<String>,
    remote_debugging_port: Option<u16>,
    headless: bool,
    engine: crate::browser::BrowserType,
    intent: ManagedLaunchIntent,
  ) -> Result<BrowserProfile, Box<dyn std::error::Error + Send + Sync>> {
    let _intent = intent;
    // Wayfern starts (and PID-reconciles) its own local proxy
    // inside `launch_browser_internal`, so we hand it None here rather than
    // staging a second, orphaned proxy worker.
    self
      .launch_browser_internal(
        app_handle,
        profile,
        url,
        None,
        remote_debugging_port,
        headless,
        engine,
      )
      .await
  }

  /// Launch one exact CFT instance and return opaque handles for an operator
  /// to explicitly bind when a legacy profile has multiple restored Grok tabs.
  /// No target is selected implicitly by this flow.
  async fn reconcile_stale_managed_target_binding(
    &self,
    profile: &BrowserProfile,
  ) -> Result<(), String> {
    let ledger = read_managed_target_binding_ledger(self.profile_manager, profile);
    if ledger
      .as_ref()
      .is_some_and(|value| value.lifecycle != "BINDING_REQUIRED")
    {
      return Ok(());
    }

    let profile_id = profile.id.to_string();
    let active_lease = crate::worker::WORKER_REGISTRY
      .list_workers()
      .await
      .workers
      .iter()
      .any(|worker| worker.profile_id == profile_id && worker.current_lease_id.is_some());
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs();

    // Keep an in-memory transition until it expires.  If it has expired, its
    // previous committed snapshot is still safe to restore because it was
    // captured before the temporary BINDING_REQUIRED ledger was written.
    let (active_binding_session, previous_committed, expired_session_ids) = {
      let sessions = TARGET_BINDING_SESSIONS.lock().await;
      let mut active = false;
      let mut previous = false;
      let mut expired = Vec::new();
      for session in sessions
        .values()
        .filter(|session| session.profile_id == profile.id)
      {
        if session.expires_at > now {
          active = true;
        } else {
          previous |= session.previous_ledger.is_some();
          expired.push(session.binding_session_id.clone());
        }
      }
      (active, previous, expired)
    };
    let action = stale_binding_reconcile_action(
      ledger.as_ref(),
      profile.process_id,
      active_lease,
      active_binding_session,
      previous_committed,
      has_managed_marker_metadata(profile),
    );
    if action == StaleBindingReconcileAction::Keep {
      return Ok(());
    }

    if !expired_session_ids.is_empty() {
      let mut sessions = TARGET_BINDING_SESSIONS.lock().await;
      for session_id in &expired_session_ids {
        sessions.remove(session_id);
      }
    }

    let previous = if previous_committed {
      let sessions = TARGET_BINDING_SESSIONS.lock().await;
      sessions
        .values()
        .find(|session| session.profile_id == profile.id && session.expires_at <= now)
        .and_then(|session| session.previous_ledger.clone())
    } else {
      None
    };
    if let Some(previous) = previous {
      persist_managed_target_binding_ledger(self.profile_manager, profile, &previous)?;
      restore_managed_marker_metadata(self.profile_manager, profile, &previous)?;
      log::info!(
        "STALE_BINDING_SESSION_RECONCILED profile={} action=RESTORE_COMMITTED",
        profile.id
      );
    } else {
      remove_managed_target_binding_ledger(self.profile_manager, profile)?;
      if has_managed_marker_metadata(profile) {
        clear_managed_marker_metadata(self.profile_manager, profile)?;
        log::info!(
          "ORPHAN_MARKER_METADATA_RECONCILED profile={} action=CLEAR_CACHE",
          profile.id
        );
      }
      if action == StaleBindingReconcileAction::RemoveTemporary {
        log::info!(
          "STALE_BINDING_SESSION_RECONCILED profile={} action=REMOVE_TEMPORARY",
          profile.id
        );
      }
    }
    Ok(())
  }

  /// Repair only the URL carrier for an already committed binding when the
  /// exact browser/target identity is still authoritative and the marker in
  /// `window.name` already matches the ledger.  Any ambiguity is deferred so
  /// startup never rewrites an unrelated Grok tab.
  pub async fn reconcile_committed_marker_carriers(&self, profiles: &[BrowserProfile]) {
    for profile in profiles {
      let Some(ledger) = read_managed_target_binding_ledger(self.profile_manager, profile) else {
        continue;
      };
      if ledger.lifecycle != "COMMITTED"
        || ledger.profile_id != profile.id.to_string()
        || profile.managed_grok_marker_id.as_deref()
          != Some(ledger.managed_target_binding_id.as_str())
        || profile.managed_grok_target_id.as_deref() != Some(ledger.last_known_target_id.as_str())
        || profile.process_id != Some(ledger.browser_pid)
        || profile.last_launch != Some(ledger.launch_generation)
      {
        continue;
      }
      let executable = PathBuf::from(&ledger.executable);
      if !chromium_spawn_identity_matches(
        profile,
        &self.profile_manager.get_profiles_dir(),
        ledger.browser_pid,
        ledger.cdp_port,
        &executable,
      ) {
        continue;
      }
      let Ok(pages) = list_cdp_pages(ledger.cdp_port).await else {
        continue;
      };
      let Some(target) = pages
        .into_iter()
        .find(|page| page.id == ledger.last_known_target_id && is_grok_url(&page.url))
      else {
        continue;
      };
      let Ok(Some(marker)) = read_marker(&target).await else {
        continue;
      };
      if marker.marker_id != ledger.managed_target_binding_id
        || marker.profile_id != ledger.profile_id
        || marker.browser_pid != ledger.browser_pid
        || marker.launch_generation != ledger.launch_generation
      {
        continue;
      }
      let Ok(current_href) = read_target_href(&target).await else {
        continue;
      };
      let Ok(parsed) = url::Url::parse(&current_href) else {
        continue;
      };
      if !is_grok_url(&current_href) {
        continue;
      }
      let expected_fragment = &managed_marker_fragment(&ledger.managed_target_binding_id)[1..];
      let current_fragment = parsed.fragment().unwrap_or_default();
      let url_matches = current_fragment == expected_fragment;
      let history_matches =
        navigation_history_matches_marker(&target, &ledger.managed_target_binding_id)
          .await
          .unwrap_or(false);
      if url_matches && history_matches {
        continue;
      }
      if !current_fragment.is_empty() && !current_fragment.starts_with(MANAGED_GROK_FRAGMENT_PREFIX)
      {
        continue;
      }
      if !replace_marker_url(&target, &ledger.managed_target_binding_id)
        .await
        .unwrap_or(false)
        || !marker_carriers_match(
          &target,
          &ManagedGrokMarker {
            version: ledger.binding_version,
            marker_id: ledger.managed_target_binding_id.clone(),
            profile_id: ledger.profile_id.clone(),
            browser_pid: ledger.browser_pid,
            launch_generation: ledger.launch_generation,
            transaction_id: marker.transaction_id.clone(),
          },
        )
        .await
        .unwrap_or(false)
      {
        continue;
      }
      capture_marker_checkpoint(
        self.profile_manager,
        profile,
        &target,
        &ledger.managed_target_binding_id,
        Some(ledger.launch_generation),
        "COMMITTED_MARKER_CARRIERS_RECONCILED",
        tokio::time::Instant::now(),
      )
      .await;
      log::info!(
        "COMMITTED_MARKER_CARRIERS_RECONCILED profile={} target_hash={}",
        profile.id,
        target_id_hash(&target.id)
      );
    }
  }

  async fn resume_pending_binding_from_ledger(
    &self,
    profile: &BrowserProfile,
    ledger: &ManagedTargetBindingLedger,
  ) -> Result<TargetBindingPrepareResponse, String> {
    if ledger.lifecycle != "BINDING_REQUIRED" {
      return Err("TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".into());
    }
    if ledger.profile_id != profile.id.to_string() || ledger.candidates.is_empty() {
      return Err("TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".into());
    }
    if !chromium_receipt_matches_binding(self.profile_manager, profile, ledger) {
      return Err("TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".into());
    }
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs();
    if ledger.expires_at.is_some_and(|expires| now > expires) {
      return Err("TARGET_BINDING_SESSION_EXPIRED".into());
    }
    let expected_executable = PathBuf::from(&ledger.executable);
    if !chromium_spawn_identity_matches(
      profile,
      &self.profile_manager.get_profiles_dir(),
      ledger.browser_pid,
      ledger.cdp_port,
      &expected_executable,
    ) || profile.last_launch != Some(ledger.launch_generation)
    {
      return Err("TARGET_BINDING_IDENTITY_CHANGED".into());
    }
    let pages = list_cdp_pages(ledger.cdp_port)
      .await
      .map_err(|_| "TARGET_BINDING_HANDLE_STALE".to_string())?;
    let live_candidates = ledger
      .candidates
      .iter()
      .filter(|candidate| {
        pages
          .iter()
          .any(|page| page.id == candidate.target_id && is_grok_url(&page.url))
      })
      .count();
    if live_candidates == 0 {
      return Err("TARGET_BINDING_HANDLE_STALE".into());
    }
    let session = TargetBindingSession {
      binding_session_id: ledger.managed_target_binding_id.clone(),
      profile_id: profile.id,
      browser_pid: ledger.browser_pid,
      cdp_port: ledger.cdp_port,
      launch_generation: ledger.launch_generation,
      executable: ledger.executable.clone(),
      user_data_dir: ledger.user_data_dir.clone(),
      expires_at: ledger.expires_at.unwrap_or(now),
      owns_browser: true,
      candidates: ledger.candidates.clone(),
      previous_ledger: ledger.previous_ledger.as_deref().cloned(),
    };
    TARGET_BINDING_SESSIONS
      .lock()
      .await
      .insert(session.binding_session_id.clone(), session);
    Ok(target_binding_prepare_response_from_ledger(ledger))
  }

  pub async fn resume_managed_grok_binding(
    &self,
    profile_id: &str,
  ) -> Result<TargetBindingPrepareResponse, String> {
    let _guard = lock_profile_launch(profile_id).await;
    if let Some(session) = TARGET_BINDING_SESSIONS
      .lock()
      .await
      .values()
      .find(|session| session.profile_id.to_string() == profile_id)
      .cloned()
    {
      return Ok(target_binding_prepare_response_from_session(&session));
    }
    let profile = self
      .profile_manager
      .list_profiles()
      .map_err(|error| error.to_string())?
      .into_iter()
      .find(|profile| profile.id.to_string() == profile_id)
      .ok_or_else(|| "TARGET_BINDING_PROFILE_NOT_FOUND".to_string())?;
    let ledger = read_managed_target_binding_ledger(self.profile_manager, &profile)
      .ok_or_else(|| "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".to_string())?;
    self
      .resume_pending_binding_from_ledger(&profile, &ledger)
      .await
  }

  pub async fn prepare_managed_grok_binding(
    &self,
    app_handle: tauri::AppHandle,
    profile: BrowserProfile,
  ) -> Result<TargetBindingPrepareResponse, String> {
    let _guard = lock_profile_launch(&profile.id.to_string()).await;
    if !is_chrome_for_testing_profile(&profile) {
      return Err("TARGET_BINDING_REQUIRES_CHROMIUM".into());
    }
    let profiles = self
      .profile_manager
      .list_profiles()
      .map_err(|error| error.to_string())?;
    let current = profiles
      .into_iter()
      .find(|candidate| candidate.id == profile.id)
      .unwrap_or(profile.clone());
    if let Some(ledger) = read_managed_target_binding_ledger(self.profile_manager, &current) {
      if ledger.lifecycle == "BINDING_REQUIRED" {
        return self
          .resume_pending_binding_from_ledger(&current, &ledger)
          .await;
      }
    }
    self
      .reconcile_stale_managed_target_binding(&current)
      .await?;
    let current_id = current.id;
    let running = current.process_id.is_some()
      && self
        .check_browser_status(app_handle.clone(), &current)
        .await
        .map_err(|error| error.to_string())?;
    let (launched, port, owns_browser) = if running {
      let profile_path = crate::ephemeral_dirs::get_effective_profile_path(
        &current,
        &self.profile_manager.get_profiles_dir(),
      )
      .join("floword-chromium")
      .join(".floword-cdp-port");
      let port = current
        .managed_grok_cdp_port
        .or_else(|| {
          std::fs::read_to_string(profile_path)
            .ok()
            .and_then(|value| value.trim().parse().ok())
        })
        .filter(|port| *port != 0)
        .ok_or_else(|| "TARGET_BINDING_CDP_PORT_MISSING".to_string())?;
      (current.clone(), port, false)
    } else {
      let port = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| error.to_string())?
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();
      remove_chromium_launch_receipt(self.profile_manager, &current).map_err(|error| {
        target_binding_prepare_failure_json(
          "TARGET_BINDING_RECEIPT_RESET_FAILED",
          "PROCESS_SPAWN",
          &error,
          false,
          false,
          false,
        )
      })?;
      let launched = match self
        .launch_browser_with_debugging_intent(
          app_handle,
          &current,
          None,
          Some(port),
          false,
          crate::browser::BrowserType::Chromium,
          ManagedLaunchIntent::BindingPrepare,
        )
        .await
      {
        Ok(launched) => launched,
        Err(error) => {
          let receipt = read_chromium_launch_receipt(self.profile_manager, &current);
          let process_spawned = receipt.is_some();
          let mut rollback_attempted = false;
          let mut rollback_succeeded = false;
          if let Some(receipt) = receipt {
            let mut rollback_profile = current.clone();
            rollback_profile.browser = "chromium".to_string();
            rollback_profile.process_id = Some(receipt.browser_pid);
            rollback_profile.last_launch = Some(receipt.launch_generation);
            rollback_attempted = true;
            rollback_succeeded =
              rollback_chromium_launch(&mut rollback_profile, receipt.cdp_port.unwrap_or(port))
                .await
                .unwrap_or(false);
          }
          return Err(target_binding_prepare_failure_json(
            if process_spawned {
              "TARGET_BINDING_PROCESS_POST_SPAWN_FAILED"
            } else {
              "TARGET_BINDING_PROCESS_SPAWN_FAILED"
            },
            "PROCESS_SPAWN",
            &error.to_string(),
            process_spawned,
            rollback_attempted,
            rollback_succeeded,
          ));
        }
      };
      (launched, port, true)
    };
    let launched = self
      .profile_manager
      .list_profiles()
      .map_err(|error| error.to_string())?
      .into_iter()
      .find(|candidate| candidate.id == current_id)
      .unwrap_or(launched);
    let browser_pid = launched.process_id.ok_or_else(|| {
      target_binding_prepare_failure_json(
        "TARGET_BINDING_BROWSER_IDENTITY_MISSING",
        "CDP_READINESS",
        "launched browser did not report a process identifier",
        owns_browser,
        false,
        false,
      )
    })?;
    let generation = launched.last_launch.ok_or_else(|| {
      target_binding_prepare_failure_json(
        "TARGET_BINDING_BROWSER_IDENTITY_MISSING",
        "CDP_READINESS",
        "launched browser did not report a launch generation",
        owns_browser,
        false,
        false,
      )
    })?;
    if let Err(error) = wait_for_cdp_ready(port, Duration::from_secs(15)).await {
      let mut rollback_attempted = false;
      let mut rollback_succeeded = false;
      if owns_browser {
        rollback_attempted = true;
        let mut rollback_profile = launched.clone();
        rollback_succeeded = rollback_chromium_launch(&mut rollback_profile, port)
          .await
          .unwrap_or(false);
      }
      return Err(target_binding_prepare_failure_json(
        "TARGET_BINDING_CDP_READINESS_FAILED",
        "CDP_READINESS",
        &error,
        owns_browser,
        rollback_attempted,
        rollback_succeeded,
      ));
    }
    let (pages, _, _) = match stabilized_cdp_pages(port, Duration::from_secs(15)).await {
      Ok(value) => value,
      Err(error) => {
        let mut rollback_attempted = false;
        let mut rollback_succeeded = false;
        if owns_browser {
          rollback_attempted = true;
          let mut rollback_profile = launched.clone();
          rollback_succeeded = rollback_chromium_launch(&mut rollback_profile, port)
            .await
            .unwrap_or(false);
        }
        return Err(target_binding_prepare_failure_json(
          "TARGET_BINDING_CDP_READINESS_FAILED",
          "CDP_READINESS",
          &error,
          owns_browser,
          rollback_attempted,
          rollback_succeeded,
        ));
      }
    };
    let candidates = pages
      .iter()
      .filter(|page| is_grok_url(&page.url))
      .map(|page| {
        let parsed = url::Url::parse(&page.url).ok();
        TargetBindingCandidate {
          handle: uuid::Uuid::new_v4().to_string(),
          target_id: page.id.clone(),
          target_id_hash: target_id_hash(&page.id),
          normalized_url: normalized_public_grok_url(&page.url),
          hostname: parsed
            .as_ref()
            .and_then(|value| value.host_str())
            .unwrap_or("grok.com")
            .to_string(),
          normalized_path: parsed
            .as_ref()
            .map(|value| value.path().to_string())
            .unwrap_or_else(|| "/".into()),
          title_hash: title_hash(&page.title),
        }
      })
      .collect::<Vec<_>>();
    let binding_session_id = uuid::Uuid::new_v4().to_string();
    let expires_at = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs()
      .saturating_add(300);
    let profile_path = crate::ephemeral_dirs::get_effective_profile_path(
      &launched,
      &self.profile_manager.get_profiles_dir(),
    )
    .join("floword-chromium");
    let executable =
      crate::browser::ChromiumBrowser::resolve_executable().map_err(|error| error.to_string())?;
    let session = TargetBindingSession {
      binding_session_id: binding_session_id.clone(),
      profile_id: launched.id,
      browser_pid,
      cdp_port: port,
      launch_generation: generation,
      executable: executable.to_string_lossy().to_string(),
      user_data_dir: profile_path.to_string_lossy().to_string(),
      expires_at,
      owns_browser,
      candidates: candidates.clone(),
      previous_ledger: read_managed_target_binding_ledger(self.profile_manager, &launched),
    };
    let ledger = ManagedTargetBindingLedger {
      profile_id: launched.id.to_string(),
      managed_target_binding_id: binding_session_id.clone(),
      last_known_target_id: "PENDING".into(),
      browser_pid,
      cdp_port: port,
      launch_generation: generation,
      managed_grok_page_url: "PENDING".into(),
      binding_created_at: expires_at.saturating_sub(300),
      binding_version: 1,
      lifecycle: "BINDING_REQUIRED".into(),
      executable: session.executable.clone(),
      user_data_dir: session.user_data_dir.clone(),
      expires_at: Some(expires_at),
      candidates: candidates.clone(),
      previous_ledger: session.previous_ledger.clone().map(Box::new),
    };
    if let Err(error) =
      persist_managed_target_binding_ledger(self.profile_manager, &launched, &ledger)
    {
      let mut rollback_profile = launched.clone();
      let _ = rollback_chromium_launch(&mut rollback_profile, port).await;
      if let Some(previous) = session.previous_ledger.as_ref() {
        let _ = persist_managed_target_binding_ledger(self.profile_manager, &launched, previous);
      } else {
        let _ = remove_managed_target_binding_ledger(self.profile_manager, &launched);
      }
      return Err(error);
    }
    TARGET_BINDING_SESSIONS
      .lock()
      .await
      .insert(binding_session_id.clone(), session);
    Ok(TargetBindingPrepareResponse {
      binding_required: true,
      binding_session_id,
      browser_pid,
      remote_debugging_port: port,
      launch_generation: generation,
      candidate_count: candidates.len(),
      candidates: candidates
        .into_iter()
        .map(|candidate| TargetBindingCandidateResponse {
          handle: candidate.handle,
          target_id_hash: candidate.target_id_hash,
          url: candidate.normalized_url,
          hostname: candidate.hostname,
          normalized_path: candidate.normalized_path,
          title_hash: candidate.title_hash,
        })
        .collect(),
    })
  }

  pub async fn confirm_managed_grok_binding(
    &self,
    binding_session_id: &str,
    handle: &str,
  ) -> Result<TargetBindingConfirmResponse, String> {
    let profile_manager = self.profile_manager;
    let session = if let Some(session) = TARGET_BINDING_SESSIONS
      .lock()
      .await
      .get(binding_session_id)
      .cloned()
    {
      session
    } else {
      let (profile, ledger) = find_durable_binding_session(profile_manager, binding_session_id)
        .ok_or_else(|| "TARGET_BINDING_SESSION_NOT_FOUND".to_string())?;
      // Rehydrate the exact persisted handles after a runtime restart. This
      // path never mints a new session id or candidate handle.
      self
        .resume_pending_binding_from_ledger(&profile, &ledger)
        .await?;
      TARGET_BINDING_SESSIONS
        .lock()
        .await
        .get(binding_session_id)
        .cloned()
        .ok_or_else(|| "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".to_string())?
    };
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .unwrap_or_default()
      .as_secs();
    if now > session.expires_at {
      TARGET_BINDING_SESSIONS
        .lock()
        .await
        .remove(binding_session_id);
      let expired_profile = profile_manager.list_profiles().ok().and_then(|profiles| {
        profiles
          .into_iter()
          .find(|profile| profile.id == session.profile_id)
      });
      if let Some(mut profile) = expired_profile {
        if session.owns_browser
          && chromium_spawn_identity_matches(
            &profile,
            &profile_manager.get_profiles_dir(),
            session.browser_pid,
            session.cdp_port,
            Path::new(&session.executable),
          )
        {
          let _ = rollback_chromium_launch(&mut profile, session.cdp_port).await;
        }
        if let Some(previous) = session.previous_ledger.as_ref() {
          let _ = persist_managed_target_binding_ledger(profile_manager, &profile, previous);
        } else {
          let _ = remove_managed_target_binding_ledger(profile_manager, &profile);
        }
        let _ = remove_chromium_launch_receipt(profile_manager, &profile);
      }
      return Err("TARGET_BINDING_SESSION_EXPIRED".into());
    }
    let _guard = lock_profile_launch(&session.profile_id.to_string()).await;
    let mut profile = profile_manager
      .list_profiles()
      .map_err(|error| error.to_string())?
      .into_iter()
      .find(|profile| profile.id == session.profile_id)
      .ok_or_else(|| "TARGET_BINDING_PROFILE_NOT_FOUND".to_string())?;
    let expected_executable = PathBuf::from(&session.executable);
    if !chromium_spawn_identity_matches(
      &profile,
      &profile_manager.get_profiles_dir(),
      session.browser_pid,
      session.cdp_port,
      &expected_executable,
    ) || profile.last_launch != Some(session.launch_generation)
    {
      return Err("TARGET_BINDING_IDENTITY_CHANGED".into());
    }
    let candidate = session
      .candidates
      .iter()
      .find(|candidate| candidate.handle == handle)
      .cloned()
      .ok_or_else(|| "TARGET_BINDING_HANDLE_INVALID".to_string())?;
    let target = list_cdp_pages(session.cdp_port)
      .await
      .map_err(|error| error.to_string())?
      .into_iter()
      .find(|target| target.id == candidate.target_id && is_grok_url(&target.url))
      .ok_or_else(|| "TARGET_BINDING_HANDLE_STALE".to_string())?;
    let marker = ManagedGrokMarker {
      version: 1,
      marker_id: session.binding_session_id.clone(),
      profile_id: profile.id.to_string(),
      browser_pid: session.browser_pid,
      launch_generation: session.launch_generation,
      transaction_id: uuid::Uuid::new_v4().to_string(),
    };
    let previous_marker = read_marker(&target).await?;
    let previous_href = read_target_href(&target).await?;
    let checkpoint_started = tokio::time::Instant::now();
    capture_marker_checkpoint(
      profile_manager,
      &profile,
      &target,
      &marker.marker_id,
      Some(marker.launch_generation),
      "MARKER_WRITE_BEFORE",
      checkpoint_started,
    )
    .await;
    let write_succeeded = write_marker(&target, &marker).await.unwrap_or(false);
    tokio::time::sleep(Duration::from_millis(100)).await;
    if !write_succeeded
      || !marker_carriers_match(&target, &marker)
        .await
        .unwrap_or(false)
    {
      let _ = restore_marker_carriers(&target, previous_marker.as_ref(), &previous_href).await;
      return Err("MANAGED_GROK_MARKER_WRITE_FAILED".into());
    }
    capture_marker_checkpoint(
      profile_manager,
      &profile,
      &target,
      &marker.marker_id,
      Some(marker.launch_generation),
      "MARKER_WRITE_AFTER",
      checkpoint_started,
    )
    .await;
    capture_marker_checkpoint(
      profile_manager,
      &profile,
      &target,
      &marker.marker_id,
      Some(marker.launch_generation),
      "MARKER_STABLE_BEFORE_STOP",
      checkpoint_started,
    )
    .await;
    let previous_profile = profile.clone();
    profile.managed_grok_marker_version = Some(1);
    profile.managed_grok_marker_id = Some(session.binding_session_id.clone());
    profile.managed_grok_marker_created_at = Some(now);
    profile.managed_grok_target_id = Some(target.id.clone());
    profile.managed_grok_browser_pid = Some(session.browser_pid);
    profile.managed_grok_cdp_port = Some(session.cdp_port);
    profile.managed_grok_launch_generation = Some(session.launch_generation);
    let ledger = ManagedTargetBindingLedger {
      profile_id: profile.id.to_string(),
      managed_target_binding_id: session.binding_session_id.clone(),
      last_known_target_id: target.id.clone(),
      browser_pid: session.browser_pid,
      cdp_port: session.cdp_port,
      launch_generation: session.launch_generation,
      managed_grok_page_url: normalized_public_grok_url(&target.url),
      binding_created_at: now,
      binding_version: 1,
      lifecycle: "COMMITTED".into(),
      executable: session.executable.clone(),
      user_data_dir: session.user_data_dir.clone(),
      expires_at: None,
      candidates: vec![],
      previous_ledger: None,
    };
    if let Err(error) = persist_managed_target_binding_ledger(profile_manager, &profile, &ledger)
      .and_then(|_| {
        profile_manager
          .save_profile(&profile)
          .map_err(|error| error.to_string())
      })
    {
      let _ = restore_marker_carriers(&target, previous_marker.as_ref(), &previous_href).await;
      profile_manager
        .save_profile(&previous_profile)
        .map_err(|save_error| format!("{error}; rollback failed: {save_error}"))?;
      if let Some(previous) = session.previous_ledger.as_ref() {
        let _ = persist_managed_target_binding_ledger(profile_manager, &profile, previous);
      } else {
        let _ = remove_managed_target_binding_ledger(profile_manager, &profile);
      }
      return Err(error);
    }
    TARGET_BINDING_SESSIONS
      .lock()
      .await
      .remove(binding_session_id);
    remove_chromium_launch_receipt(profile_manager, &profile)?;
    Ok(TargetBindingConfirmResponse {
      profile_id: profile.id.to_string(),
      binding_session_id: session.binding_session_id,
      target_id_hash: target_id_hash(&target.id),
      browser_pid: session.browser_pid,
      cdp_port: session.cdp_port,
      launch_generation: session.launch_generation,
      lifecycle: "COMMITTED".into(),
    })
  }

  pub async fn abort_managed_grok_binding(
    &self,
    binding_session_id: &str,
  ) -> Result<TargetBindingAbortResponse, String> {
    let session = if let Some(session) = TARGET_BINDING_SESSIONS
      .lock()
      .await
      .get(binding_session_id)
      .cloned()
    {
      session
    } else {
      let (profile, ledger) =
        find_durable_binding_session(self.profile_manager, binding_session_id)
          .ok_or_else(|| "TARGET_BINDING_SESSION_NOT_FOUND".to_string())?;
      self
        .resume_pending_binding_from_ledger(&profile, &ledger)
        .await?;
      TARGET_BINDING_SESSIONS
        .lock()
        .await
        .get(binding_session_id)
        .cloned()
        .ok_or_else(|| "TARGET_BINDING_RESPONSE_NOT_RECOVERABLE".to_string())?
    };
    let mut profile = self
      .profile_manager
      .list_profiles()
      .map_err(|error| error.to_string())?
      .into_iter()
      .find(|profile| profile.id == session.profile_id)
      .ok_or_else(|| "TARGET_BINDING_PROFILE_NOT_FOUND".to_string())?;
    if !chromium_spawn_identity_matches(
      &profile,
      &self.profile_manager.get_profiles_dir(),
      session.browser_pid,
      session.cdp_port,
      Path::new(&session.executable),
    ) {
      return Err("TARGET_BINDING_IDENTITY_CHANGED".into());
    }
    let stopped = if session.owns_browser {
      rollback_chromium_launch(&mut profile, session.cdp_port).await?
    } else {
      false
    };
    if let Some(previous) = session.previous_ledger.as_ref() {
      persist_managed_target_binding_ledger(self.profile_manager, &profile, previous)?;
      restore_managed_marker_metadata(self.profile_manager, &profile, previous)?;
    } else {
      remove_managed_target_binding_ledger(self.profile_manager, &profile)?;
      clear_managed_marker_metadata(self.profile_manager, &profile)?;
    }
    TARGET_BINDING_SESSIONS
      .lock()
      .await
      .remove(binding_session_id);
    remove_chromium_launch_receipt(self.profile_manager, &profile)?;
    Ok(TargetBindingAbortResponse {
      binding_session_id: session.binding_session_id,
      lifecycle: "ABORTED".into(),
      browser_stopped: stopped,
    })
  }

  pub async fn launch_or_open_url(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
    url: Option<String>,
    internal_proxy_settings: Option<&ProxySettings>,
  ) -> Result<BrowserProfile, Box<dyn std::error::Error + Send + Sync>> {
    log::info!(
      "launch_or_open_url called for profile: {} (ID: {})",
      profile.name,
      profile.id
    );

    // Get the most up-to-date profile data
    let profiles = self
      .profile_manager
      .list_profiles()
      .map_err(|e| format!("Failed to list profiles in launch_or_open_url: {e}"))?;
    let updated_profile = profiles
      .into_iter()
      .find(|p| p.id == profile.id)
      .unwrap_or_else(|| profile.clone());

    log::info!(
      "Checking browser status for profile: {} (ID: {})",
      updated_profile.name,
      updated_profile.id
    );

    // Check if browser is already running
    let is_running = self
      .check_browser_status(app_handle.clone(), &updated_profile)
      .await
      .map_err(|e| format!("Failed to check browser status: {e}"))?;

    // Get the updated profile again after status check (PID might have been updated)
    let profiles = self
      .profile_manager
      .list_profiles()
      .map_err(|e| format!("Failed to list profiles after status check: {e}"))?;
    let final_profile = profiles
      .into_iter()
      .find(|p| p.id == profile.id)
      .unwrap_or_else(|| updated_profile.clone());

    log::info!(
      "Browser status check: running={is_running}, URL requested={}, PID present={}",
      url.is_some(),
      final_profile.process_id.is_some()
    );

    if is_running {
      if let Some(url_ref) = url.as_ref() {
        log::info!(
          "Opening {} in existing browser",
          crate::log_redaction::url_label(url_ref)
        );

        match self
          .open_url_in_existing_browser(
            app_handle.clone(),
            &final_profile,
            url_ref,
            internal_proxy_settings,
          )
          .await
        {
          Ok(()) => {
            log::info!("Successfully opened URL in existing browser");
            Ok(final_profile)
          }
          Err(e) => {
            log::info!(
              "Failed to open URL in existing browser: {}",
              crate::log_redaction::text(&e.to_string())
            );
            Err(e)
          }
        }
      } else {
        log::info!("Browser is already running and no URL was requested");
        Ok(final_profile)
      }
    } else {
      log::info!("Launching new browser instance - browser not running");
      self
        .launch_browser_internal(
          app_handle.clone(),
          &final_profile,
          url,
          internal_proxy_settings,
          None,
          false,
          crate::browser::BrowserType::Wayfern,
        )
        .await
    }
  }

  fn save_process_info(
    &self,
    profile: &BrowserProfile,
  ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Use the regular save_profile method which handles the UUID structure
    self.profile_manager.save_profile(profile).map_err(|e| {
      let error_string = e.to_string();
      Box::new(std::io::Error::other(error_string)) as Box<dyn std::error::Error + Send + Sync>
    })
  }

  pub async fn check_browser_status(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
  ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    self
      .profile_manager
      .check_browser_status(app_handle, profile)
      .await
  }

  pub async fn kill_browser_process(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
  ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _profile_launch_guard = lock_profile_launch(&profile.id.to_string()).await;
    self
      .kill_browser_process_unlocked(app_handle, profile)
      .await
  }

  /// Stop a Floword-managed Chromium/CFT session and return a sanitized
  /// lifecycle receipt.  The legacy kill method remains `Result<()>` for
  /// existing callers; API routes that need proof use this method.
  pub async fn stop_browser_process_with_result(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
  ) -> Result<StopBrowserResult, Box<dyn std::error::Error + Send + Sync>> {
    let _profile_launch_guard = lock_profile_launch(&profile.id.to_string()).await;
    if !is_chrome_for_testing_profile(profile) {
      self
        .kill_browser_process_unlocked(app_handle, profile)
        .await?;
      return Ok(StopBrowserResult {
        profile_id: profile.id.to_string(),
        ok: true,
        browser_engine: crate::browser::BrowserType::from_str(&profile.browser)
          .map(|engine| engine.canonical_engine_name().to_string())
          .unwrap_or_else(|_| "WAYFERN".to_string()),
        stopped_pid: 0,
        launch_generation: 0,
        graceful: false,
      });
    }

    let workers = crate::worker::WORKER_REGISTRY.list_workers().await;
    if workers.workers.iter().any(|worker| {
      worker.profile_id == profile.id.to_string() && worker.current_lease_id.is_some()
    }) {
      return Err("PROFILE_BUSY".into());
    }

    let profiles_dir = self.profile_manager.get_profiles_dir();
    // The launch receipt is the authoritative executable identity for the
    // live session.  Re-resolving from the current process environment can
    // select a different staged CFT tree after an ArtCraft activation, even
    // though the profile/PID/CDP session is still valid.  Only accept the
    // receipt when all persisted identity fields still match this profile;
    // otherwise fail closed rather than guessing which browser to stop.
    let executable = read_chromium_launch_receipt(self.profile_manager, profile)
      .filter(|receipt| {
        receipt.profile_id == profile.id.to_string()
          && receipt.browser_pid == profile.process_id.unwrap_or_default()
          && receipt.launch_generation == profile.last_launch.unwrap_or_default()
          && receipt.cdp_port == profile.managed_grok_cdp_port
      })
      .map(|receipt| PathBuf::from(receipt.executable))
      .or_else(|| crate::browser::ChromiumBrowser::resolve_executable().ok())
      .ok_or("BROWSER_SESSION_IDENTITY_MISMATCH")?;
    let (pid, port, _generation) =
      chromium_process_identity_matches(profile, &profiles_dir, &executable)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;

    let mut graceful = close_cdp_browser(port).await.is_ok();
    let mut stopped = wait_for_process_exit(pid, Duration::from_secs(5)).await;
    if !stopped {
      // Re-check the complete identity before any fallback termination.  A
      // reused PID or changed command line must never be force-killed.
      chromium_process_identity_matches(profile, &profiles_dir, &executable)
        .map_err(|error| -> Box<dyn std::error::Error + Send + Sync> { error.into() })?;
      #[cfg(target_os = "windows")]
      crate::platform_browser::windows::kill_browser_process_impl(pid).await?;
      #[cfg(target_os = "macos")]
      crate::platform_browser::macos::kill_browser_process_impl(
        pid,
        Some(
          &crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
            .join("floword-chromium")
            .to_string_lossy(),
        ),
      )
      .await?;
      #[cfg(target_os = "linux")]
      crate::platform_browser::linux::kill_browser_process_impl(
        pid,
        Some(
          &crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir)
            .join("floword-chromium")
            .to_string_lossy(),
        ),
      )
      .await?;
      stopped = wait_for_process_exit(pid, Duration::from_secs(3)).await;
      graceful = false;
    }
    if !stopped {
      return Err("BROWSER_STOP_FAILED".into());
    }

    let mut updated = profile.clone();
    updated.process_id = None;
    updated.managed_grok_target_id = None;
    updated.managed_grok_browser_pid = None;
    updated.managed_grok_cdp_port = None;
    updated.managed_grok_launch_generation = None;
    self.save_process_info(&updated)?;
    let payload = serde_json::json!({
      "id": updated.id.to_string(),
      "is_running": false,
    });
    let _ = events::emit("profile-running-changed", &payload);
    let _ = events::emit("profile-updated", &updated);

    Ok(StopBrowserResult {
      profile_id: profile.id.to_string(),
      ok: true,
      browser_engine: "CHROME_FOR_TESTING".to_string(),
      stopped_pid: 0,
      launch_generation: 0,
      graceful,
    })
  }

  async fn kill_browser_process_unlocked(
    &self,
    app_handle: tauri::AppHandle,
    profile: &BrowserProfile,
  ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Handle Wayfern profiles using WayfernManager
    if profile.browser == "wayfern" {
      let profiles_dir = self.profile_manager.get_profiles_dir();
      let profile_data_path =
        crate::ephemeral_dirs::get_effective_profile_path(profile, &profiles_dir);
      let profile_path_str = profile_data_path.to_string_lossy();

      log::info!(
        "Attempting to kill Wayfern process for profile: {} (ID: {})",
        profile.name,
        profile.id
      );

      // Stop the proxy associated with this profile first
      let profile_id_str = profile.id.to_string();
      if let Err(e) = PROXY_MANAGER
        .stop_proxy_by_profile_id(app_handle.clone(), &profile_id_str)
        .await
      {
        log::warn!(
          "Warning: Failed to stop proxy for profile {}: {e}",
          profile_id_str
        );
      }
      if let Err(error) =
        crate::xray_worker_runner::stop_xray_worker_by_profile_id(&profile_id_str).await
      {
        log::warn!(
          "Warning: Failed to stop Xray-core worker for profile {}: {error}",
          profile_id_str
        );
      }

      let mut process_actually_stopped = false;
      match self
        .wayfern_manager
        .find_wayfern_by_profile(&profile_path_str)
        .await
      {
        Some(wayfern_process) => {
          log::info!(
            "Found Wayfern process: {} (PID: {:?})",
            wayfern_process.id,
            wayfern_process.processId
          );

          match self.wayfern_manager.stop_wayfern(&wayfern_process.id).await {
            Ok(_) => {
              if let Some(pid) = wayfern_process.processId {
                // Verify the process actually died by checking after a short delay
                use tokio::time::{sleep, Duration};
                sleep(Duration::from_millis(500)).await;

                use sysinfo::{Pid, System};
                let system = System::new_all();
                process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();

                if process_actually_stopped {
                  log::info!(
                    "Successfully stopped Wayfern process: {} (PID: {:?}) - verified process is dead",
                    wayfern_process.id,
                    pid
                  );
                } else {
                  log::warn!(
                    "Wayfern stop command returned success but process {} (PID: {:?}) is still running - forcing kill",
                    wayfern_process.id,
                    pid
                  );
                  // Force kill the process
                  #[cfg(target_os = "macos")]
                  {
                    use crate::platform_browser;
                    if let Err(e) = platform_browser::macos::kill_browser_process_impl(
                      pid,
                      Some(&profile_path_str),
                    )
                    .await
                    {
                      log::error!("Failed to force kill Wayfern process {}: {}", pid, e);
                    } else {
                      sleep(Duration::from_millis(500)).await;
                      let system = System::new_all();
                      process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                      if process_actually_stopped {
                        log::info!(
                          "Successfully force killed Wayfern process {} (PID: {:?})",
                          wayfern_process.id,
                          pid
                        );
                      }
                    }
                  }
                  #[cfg(target_os = "linux")]
                  {
                    use crate::platform_browser;
                    if let Err(e) = platform_browser::linux::kill_browser_process_impl(
                      pid,
                      Some(&profile_path_str),
                    )
                    .await
                    {
                      log::error!("Failed to force kill Wayfern process {}: {}", pid, e);
                    } else {
                      sleep(Duration::from_millis(500)).await;
                      let system = System::new_all();
                      process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                      if process_actually_stopped {
                        log::info!(
                          "Successfully force killed Wayfern process {} (PID: {:?})",
                          wayfern_process.id,
                          pid
                        );
                      }
                    }
                  }
                  #[cfg(target_os = "windows")]
                  {
                    use crate::platform_browser;
                    if let Err(e) = platform_browser::windows::kill_browser_process_impl(pid).await
                    {
                      log::error!("Failed to force kill Wayfern process {}: {}", pid, e);
                    } else {
                      sleep(Duration::from_millis(500)).await;
                      let system = System::new_all();
                      process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                      if process_actually_stopped {
                        log::info!(
                          "Successfully force killed Wayfern process {} (PID: {:?})",
                          wayfern_process.id,
                          pid
                        );
                      }
                    }
                  }
                }
              } else {
                process_actually_stopped = true;
              }
            }
            Err(e) => {
              log::error!(
                "Error stopping Wayfern process {}: {}",
                wayfern_process.id,
                e
              );
              // Try to force kill if we have a PID
              if let Some(pid) = wayfern_process.processId {
                log::info!(
                  "Attempting force kill after stop_wayfern error for PID: {}",
                  pid
                );
                #[cfg(target_os = "macos")]
                {
                  use crate::platform_browser;
                  if let Err(kill_err) =
                    platform_browser::macos::kill_browser_process_impl(pid, Some(&profile_path_str))
                      .await
                  {
                    log::error!("Failed to force kill Wayfern process {}: {}", pid, kill_err);
                  } else {
                    use tokio::time::{sleep, Duration};
                    sleep(Duration::from_millis(500)).await;
                    use sysinfo::{Pid, System};
                    let system = System::new_all();
                    process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                  }
                }
                #[cfg(target_os = "linux")]
                {
                  use crate::platform_browser;
                  if let Err(kill_err) =
                    platform_browser::linux::kill_browser_process_impl(pid, Some(&profile_path_str))
                      .await
                  {
                    log::error!("Failed to force kill Wayfern process {}: {}", pid, kill_err);
                  } else {
                    use tokio::time::{sleep, Duration};
                    sleep(Duration::from_millis(500)).await;
                    use sysinfo::{Pid, System};
                    let system = System::new_all();
                    process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                  }
                }
                #[cfg(target_os = "windows")]
                {
                  use crate::platform_browser;
                  if let Err(kill_err) =
                    platform_browser::windows::kill_browser_process_impl(pid).await
                  {
                    log::error!("Failed to force kill Wayfern process {}: {}", pid, kill_err);
                  } else {
                    use tokio::time::{sleep, Duration};
                    sleep(Duration::from_millis(500)).await;
                    use sysinfo::{Pid, System};
                    let system = System::new_all();
                    process_actually_stopped = system.process(Pid::from(pid as usize)).is_none();
                  }
                }
              }
            }
          }
        }
        None => {
          log::info!(
            "No running Wayfern process found for profile: {} (ID: {})",
            profile.name,
            profile.id
          );
          process_actually_stopped = true;
        }
      }

      // If process wasn't confirmed stopped, return an error
      if !process_actually_stopped {
        log::error!(
          "Failed to stop Wayfern process for profile: {} (ID: {}) - process may still be running",
          profile.name,
          profile.id
        );
        return Err(
          format!(
            "Failed to stop Wayfern process for profile {} - process may still be running",
            profile.name
          )
          .into(),
        );
      }

      // Clear the process ID from the profile and save immediately so that
      // subsequent calls to update_profile_version (which re-reads from disk)
      // see the cleared process_id.
      let mut updated_profile = profile.clone();
      updated_profile.process_id = None;
      self
        .save_process_info(&updated_profile)
        .map_err(|e| format!("Failed to update profile: {e}"))?;

      // Check for pending updates and apply them
      if let Ok(Some(pending_update)) = self
        .auto_updater
        .get_pending_update(&profile.browser, &profile.version)
      {
        log::info!(
          "Found pending update for Wayfern profile {}: {} -> {}",
          profile.name,
          profile.version,
          pending_update.new_version
        );

        match self.profile_manager.update_profile_version(
          &app_handle,
          &profile.id.to_string(),
          &pending_update.new_version,
        ) {
          Ok(updated_profile_after_update) => {
            log::info!(
              "Successfully updated Wayfern profile {} from version {} to {}",
              profile.name,
              profile.version,
              pending_update.new_version
            );
            updated_profile = updated_profile_after_update;

            if let Err(e) = self
              .auto_updater
              .dismiss_update_notification(&pending_update.id)
            {
              log::warn!("Warning: Failed to dismiss pending update notification: {e}");
            }
          }
          Err(e) => {
            log::error!(
              "Failed to apply pending update for Wayfern profile {}: {}",
              profile.name,
              e
            );
          }
        }
      }

      // If no pending update was applied, check if a newer installed version exists
      if updated_profile.version == profile.version {
        if let Some(p) = self
          .auto_updater
          .update_profile_to_latest_installed(&app_handle, &updated_profile)
        {
          updated_profile = p;
        }
      }

      log::info!(
        "Emitting profile events for successful Wayfern kill: {}",
        updated_profile.name
      );

      // Emit profile update event to frontend
      if let Err(e) = events::emit("profile-updated", &updated_profile) {
        log::warn!("Warning: Failed to emit profile update event: {e}");
      }

      // Emit minimal running changed event
      #[derive(Serialize)]
      struct RunningChangedPayload {
        id: String,
        is_running: bool,
      }
      let payload = RunningChangedPayload {
        id: updated_profile.id.to_string(),
        is_running: false,
      };

      if let Err(e) = events::emit("profile-running-changed", &payload) {
        log::warn!("Warning: Failed to emit profile running changed event: {e}");
      } else {
        log::info!(
          "Successfully emitted profile-running-changed event for Wayfern {}: running={}",
          updated_profile.name,
          payload.is_running
        );
      }

      if profile.password_protected {
        // Await the re-encryption so the queued sync (released later by
        // `mark_profile_stopped` in `kill_browser`) sees fresh ciphertext on
        // disk instead of the previous snapshot.
        crate::profile::password::complete_after_quit_and_wait(profile).await;
      }

      log::info!(
        "Wayfern process cleanup completed for profile: {} (ID: {})",
        profile.name,
        profile.id
      );

      // Consolidate browser versions after stopping a browser
      if let Ok(consolidated) = self
        .downloaded_browsers_registry
        .consolidate_browser_versions(&app_handle)
      {
        if !consolidated.is_empty() {
          log::info!("Post-stop version consolidation results:");
          for action in &consolidated {
            log::info!("  {action}");
          }
        }
      }

      return Ok(());
    }

    Err(
      format!(
        "Unsupported browser '{}' for profile '{}' — only Wayfern is supported",
        profile.browser, profile.name
      )
      .into(),
    )
  }

  pub async fn open_url_with_profile(
    &self,
    app_handle: tauri::AppHandle,
    profile_id: String,
    url: String,
  ) -> Result<(), String> {
    // Get the profile by name
    let profiles = self
      .profile_manager
      .list_profiles()
      .map_err(|e| format!("Failed to list profiles: {e}"))?;
    let profile = profiles
      .into_iter()
      .find(|p| p.id.to_string() == profile_id)
      .ok_or_else(|| format!("Profile '{profile_id}' not found"))?;
    let _profile_launch_guard = lock_profile_launch(&profile.id.to_string()).await;

    if profile.is_cross_os() {
      return Err(format!(
        "Cannot open URL with profile '{}': this profile was created on {} and cannot be used on a different operating system",
        profile.name,
        profile.host_os.as_deref().unwrap_or("another OS"),
      ));
    }

    log::info!("Opening URL with selected profile");

    // Use launch_or_open_url which handles both launching new instances and opening in existing ones
    self
      .launch_or_open_url(app_handle, &profile, Some(url.clone()), None)
      .await
      .map_err(|e| {
        log::info!(
          "Failed to open URL with selected profile: {}",
          crate::log_redaction::text(&e.to_string())
        );
        format!("Failed to open URL with profile: {e}")
      })?;

    log::info!("Successfully opened URL with selected profile");
    Ok(())
  }
}

#[tauri::command]
pub async fn launch_browser_profile(
  app_handle: tauri::AppHandle,
  profile: BrowserProfile,
  url: Option<String>,
) -> Result<BrowserProfile, String> {
  launch_browser_profile_impl(app_handle, profile, url, None, false, false).await
}

pub async fn launch_browser_profile_impl(
  app_handle: tauri::AppHandle,
  profile: BrowserProfile,
  url: Option<String>,
  remote_debugging_port: Option<u16>,
  headless: bool,
  force_new: bool,
) -> Result<BrowserProfile, String> {
  let engine = crate::browser::BrowserType::from_str(&profile.browser).ok();
  launch_browser_profile_impl_with_policy_result(
    app_handle,
    profile,
    LaunchUrlPolicy::AlwaysOpen(url),
    remote_debugging_port,
    headless,
    force_new,
    engine,
  )
  .await
  .map(|result| result.profile)
}

pub async fn launch_browser_profile_impl_with_policy_result(
  app_handle: tauri::AppHandle,
  profile: BrowserProfile,
  policy: LaunchUrlPolicy,
  remote_debugging_port: Option<u16>,
  headless: bool,
  force_new: bool,
  engine: Option<crate::browser::BrowserType>,
) -> Result<FlowordLaunchResult, String> {
  log::info!(
    "Launch request received for profile: {} (ID: {})",
    profile.name,
    profile.id
  );
  if profile.is_cross_os() {
    return Err(format!(
      "Cannot launch profile '{}': this profile was created on {} and cannot be launched on a different operating system",
      profile.name,
      profile.host_os.as_deref().unwrap_or("another OS"),
    ));
  }

  // Team lock check: if profile is sync-enabled and user is on a team, acquire lock
  crate::team_lock::acquire_team_lock_if_needed(&profile).await?;

  // Notify sync scheduler that profile is now running and queue sync for when it stops
  if let Some(scheduler) = crate::sync::get_global_scheduler() {
    let pid = profile.id.to_string();
    scheduler.mark_profile_running(&pid).await;
    if profile.is_sync_enabled() {
      scheduler.queue_profile_sync(pid).await;
    }
  }

  let browser_runner = BrowserRunner::instance();

  // Resolve the most up-to-date profile from disk by ID to avoid using stale proxy_id/browser state
  let profile_for_launch = match browser_runner
    .profile_manager
    .list_profiles()
    .map_err(|e| format!("Failed to list profiles: {e}"))
  {
    Ok(profiles) => profiles
      .into_iter()
      .find(|p| p.id == profile.id)
      .unwrap_or_else(|| profile.clone()),
    Err(e) => {
      return Err(e);
    }
  };

  log::info!(
    "Resolved profile for launch: {} (ID: {})",
    profile_for_launch.name,
    profile_for_launch.id
  );

  log::info!(
    "Starting browser launch for profile: {} (ID: {})",
    profile_for_launch.name,
    profile_for_launch.id
  );

  if force_new
    && browser_runner
      .check_browser_status(app_handle.clone(), &profile_for_launch)
      .await
      .map_err(|error| {
        crate::wrap_backend_error(error, "Failed to check browser status before launch")
      })?
  {
    return Err(crate::backend_error("PROFILE_RUNNING"));
  }

  // Launch browser or open URL in existing instance. Wayfern starts its
  // own local proxy inside `launch_browser_internal`; other browser types
  // are rejected there, so no proxy needs to be staged here.
  //
  // `force_new` callers (API/MCP) always start a fresh instance with the
  // requested debug port and headless mode, bypassing the "open URL in the
  // existing window" path which would otherwise ignore both.
  let profile_id = profile_for_launch.id.to_string();
  let check_app_handle = app_handle.clone();
  let check_profile = profile_for_launch.clone();
  let launch_app_handle = app_handle.clone();
  let launch_profile = profile_for_launch.clone();
  let selected_engine = engine.unwrap_or(crate::browser::BrowserType::Wayfern);
  let check_engine = selected_engine.clone();
  let launch_engine = selected_engine.clone();
  let cold_start_floword = matches!(&policy, LaunchUrlPolicy::ColdStartOnly(_));
  let remote_debugging_port = if selected_engine == crate::browser::BrowserType::Chromium
    && remote_debugging_port.is_none()
  {
    Some(
      std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|e| e.to_string())?
        .local_addr()
        .map_err(|e| e.to_string())?
        .port(),
    )
  } else {
    remote_debugging_port
  };
  let (launch_result, reused) = launch_with_url_policy(
    &profile_id,
    policy,
    || async {
      let running = browser_runner
        .check_browser_status(check_app_handle, &check_profile)
        .await
        .map_err(|error| {
          crate::wrap_backend_error(
            error,
            "Failed to check browser status before cold-start policy",
          )
        })?;
      if check_engine == crate::browser::BrowserType::Chromium
        && check_profile.browser != "chromium"
      {
        Ok(false)
      } else {
        Ok(running)
      }
    },
    |launch_url| async move {
      // ColdStartOnly returns no URL when an existing browser is reusable.  In
      // that case never spawn a second Chromium process; return the persisted
      // profile state and let the caller report `reused=true`.
      if launch_engine == crate::browser::BrowserType::Chromium
        && !force_new
        && launch_url.is_none()
      {
        return Ok(launch_profile.clone());
      }
      if force_new || launch_engine == crate::browser::BrowserType::Chromium {
        // Floword owns Grok target reconciliation after CDP is ready.  Do not
        // put the Grok URL on the Chromium command line, otherwise session
        // restore and post-CDP reconciliation become two competing owners.
        let browser_launch_url = if launch_engine == crate::browser::BrowserType::Chromium {
          sanitize_chromium_launch_url(launch_url, cold_start_floword)
        } else {
          launch_url
        };
        browser_runner
          .launch_browser_with_debugging(
            launch_app_handle.clone(),
            &launch_profile,
            browser_launch_url,
            remote_debugging_port,
            headless,
            launch_engine.clone(),
          )
          .await
          .map_err(|error| {
            log::info!(
              "Browser launch failed for profile: {}, error: {}",
              launch_profile.name,
              error
            );
            #[derive(serde::Serialize)]
            struct RunningChangedPayload {
              id: String,
              is_running: bool,
            }
            if let Err(emit_error) = events::emit(
              "profile-running-changed",
              &RunningChangedPayload {
                id: launch_profile.id.to_string(),
                is_running: false,
              },
            ) {
              log::warn!("Warning: Failed to emit profile running changed event: {emit_error}");
            }
            let message = error.to_string();
            if message.contains("Exec format error") {
              format!(
                "Failed to launch browser: Executable format error. This browser version is not compatible with your system architecture ({}). Please try a different browser or version that supports your platform.",
                std::env::consts::ARCH
              )
            } else {
              crate::wrap_backend_error(error, "Failed to launch browser or open URL")
            }
          })
      } else {
        browser_runner
          .launch_or_open_url(launch_app_handle, &launch_profile, launch_url, None)
          .await
          .map_err(|error| {
            log::info!(
              "Browser launch failed for profile: {}, error: {}",
              launch_profile.name,
              error
            );
            #[derive(serde::Serialize)]
            struct RunningChangedPayload {
              id: String,
              is_running: bool,
            }
            if let Err(emit_error) = events::emit(
              "profile-running-changed",
              &RunningChangedPayload {
                id: launch_profile.id.to_string(),
                is_running: false,
              },
            ) {
              log::warn!("Warning: Failed to emit profile running changed event: {emit_error}");
            }
            let message = error.to_string();
            if message.contains("Exec format error") {
              format!(
                "Failed to launch browser: Executable format error. This browser version is not compatible with your system architecture ({}). Please try a different browser or version that supports your system architecture.",
                std::env::consts::ARCH
              )
            } else {
              crate::wrap_backend_error(error, "Failed to launch browser or open URL")
            }
          })
      }
    },
  )
  .await?;
  let mut updated_profile = launch_result;
  let reused = if force_new { false } else { reused };

  log::info!(
    "Browser launch completed for profile: {} (ID: {})",
    updated_profile.name,
    updated_profile.id
  );

  // The proxy PID mapping was already reconciled inside launch_browser_internal
  // (placeholder → real browser PID); nothing is ever keyed by a constant here.

  let reported_remote_debugging_port =
    if selected_engine == crate::browser::BrowserType::Chromium && reused {
      let profiles_dir = browser_runner.profile_manager.get_profiles_dir();
      let path = crate::ephemeral_dirs::get_effective_profile_path(&updated_profile, &profiles_dir)
        .join("floword-chromium")
        .join(".floword-cdp-port");
      std::fs::read_to_string(path)
        .ok()
        .and_then(|value| value.trim().parse().ok())
    } else {
      remote_debugging_port
    };
  let (grok_target_id, grok_page_url, grok_target_reused, target_selection_source) =
    if selected_engine == crate::browser::BrowserType::Chromium {
      let port = match reported_remote_debugging_port {
        Some(port) => port,
        None => {
          if !reused {
            let _ = rollback_chromium_launch(&mut updated_profile, 0).await;
          }
          return Err(crate::backend_error_with_detail(
            "RUN_POST_SPAWN_RECONCILE_FAILED",
            "CDP port was not available after browser spawn",
          ));
        }
      };
      let stale_managed_mapping = managed_mapping_is_stale(&updated_profile, port);
      let target_result = ensure_grok_target(
        port,
        Duration::from_secs(15),
        &mut updated_profile,
        browser_runner.profile_manager,
        !reused || stale_managed_mapping,
      )
      .await;
      let (target, target_reused, target_selection_source) = match target_result {
        Ok(value) => value,
        Err(error) => {
          // Capture the launch receipt before rollback clears the persisted
          // profile fields.  Error responses must retain the exact identity
          // that was spawned, even when cleanup succeeds.
          let spawned_pid = updated_profile.process_id;
          let launch_generation = updated_profile.last_launch;
          let rollback = if !reused {
            rollback_chromium_launch(&mut updated_profile, port).await
          } else {
            Ok(false)
          };
          let rollback_succeeded = rollback.unwrap_or(false);
          let stage =
            if error.contains("AMBIGUOUS_GROK_TAB") || error.contains("GROK_TARGET_SET_UNSTABLE") {
              "GROK_TARGET_SELECTION"
            } else if error.contains("GROK_TAB_NOT_FOUND") {
              "GROK_TARGET_DISCOVERY"
            } else if error.contains("CDP") {
              "CDP_READINESS"
            } else {
              "GROK_TARGET_DISCOVERY"
            };
          let mut details = serde_json::json!({
            "processSpawned": !reused,
            "rollbackAttempted": !reused,
            "rollbackSucceeded": rollback_succeeded,
            "browserPid": spawned_pid,
            "launchGeneration": launch_generation,
            "cdpPort": port,
          });
          let error_code = if error == "STALE_MANAGED_TARGET_MARKER" {
            details["durableLedgerPresent"] = serde_json::Value::Bool(true);
            details["durableMarkerMatchCount"] = serde_json::Value::from(0u64);
            details["selectionPath"] = serde_json::Value::String("DURABLE_MARKER_MISSING".into());
            "STALE_MANAGED_TARGET_MARKER"
          } else {
            "RUN_POST_SPAWN_RECONCILE_FAILED"
          };
          return Err(
            serde_json::json!({
              "code": error_code,
              "stage": stage,
              "params": { "detail": error },
              "details": details,
            })
            .to_string(),
          );
        }
      };
      (
        Some(target.id),
        Some(normalized_public_grok_url(&target.url)),
        target_reused,
        Some(target_selection_source),
      )
    } else {
      (None, None, false, None)
    };

  // Publish the worker only after CFT target reconciliation and durable
  // managed-session persistence have succeeded.  A sibling WAYFERN worker
  // cannot overwrite this authoritative browser session.
  {
    let worker_id = format!("browser-profile:{}", updated_profile.id);
    let worker = crate::worker::BrowserWorker {
      worker_id,
      profile_id: updated_profile.id.to_string(),
      provider: if selected_engine == crate::browser::BrowserType::Chromium {
        crate::worker::WorkerProvider::Playwright
      } else {
        crate::worker::WorkerProvider::Wayfern
      },
      pool_id: updated_profile.group_id.clone(),
      state: crate::worker::WorkerState::Starting,
      capabilities: vec![],
      extension_ready: false,
      extension_version: None,
      protocol_version: None,
      grok_logged_in: None,
      site_sessions: std::collections::HashMap::new(),
      site_capabilities: std::collections::HashMap::new(),
      current_lease_id: None,
      current_job_id: None,
      last_heartbeat_at: Some(chrono::Utc::now().to_rfc3339()),
      last_error: None,
    };
    if let Err(error) = crate::worker::WORKER_REGISTRY
      .register_or_update_worker(worker)
      .await
    {
      if !reused {
        let _ = rollback_chromium_launch(
          &mut updated_profile,
          reported_remote_debugging_port.unwrap_or(0),
        )
        .await;
      }
      return Err(crate::backend_error_with_detail(
        "RUN_POST_SPAWN_RECONCILE_FAILED",
        error,
      ));
    }

    let profile_id_clone = updated_profile.id.to_string();
    let worker_id_clone = format!("browser-profile:{}", updated_profile.id);
    tauri::async_runtime::spawn(async move {
      for _ in 0..15 {
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        if let Ok(handshake) =
          crate::worker::worker_routes::probe_worker_health(&profile_id_clone).await
        {
          let _ = crate::worker::WORKER_REGISTRY
            .handle_health_handshake(&worker_id_clone, handshake)
            .await;
          break;
        }
      }
    });
  }

  Ok(FlowordLaunchResult {
    profile: updated_profile,
    reused,
    remote_debugging_port: reported_remote_debugging_port,
    grok_target_id,
    grok_page_url,
    grok_target_reused,
    target_selection_source,
  })
}

async fn lock_vpn_pool_rotation(pool_id: &str) -> tokio::sync::OwnedMutexGuard<()> {
  let lock = {
    let mut locks = VPN_POOL_ROTATION_LOCKS.lock().await;
    locks
      .entry(pool_id.to_string())
      .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
      .clone()
  };
  lock.lock_owned().await
}

/// Safely rotates a pool used by live browser profiles. Every affected browser
/// is stopped before the pool changes, then relaunched only after the new VPN
/// endpoint has passed verification. If rotation fails, profiles are still
/// relaunched against the previous pool state.
pub async fn safe_rotate_vpn_pool(
  app_handle: tauri::AppHandle,
  pool_id: &str,
) -> Result<crate::vpn::pool::VpnPoolRuntime, String> {
  let _rotation_guard = lock_vpn_pool_rotation(pool_id).await;
  let runner = BrowserRunner::instance();
  let pool_reference = format!("{}{}", crate::vpn::pool::POOL_REFERENCE_PREFIX, pool_id);
  let candidates = runner
    .profile_manager
    .list_profiles()
    .map_err(|error| crate::backend_error_with_detail("INTERNAL_ERROR", error))?
    .into_iter()
    .filter(|profile| profile.vpn_id.as_deref() == Some(pool_reference.as_str()))
    .collect::<Vec<_>>();

  let mut running = Vec::new();
  for profile in candidates {
    if runner
      .check_browser_status(app_handle.clone(), &profile)
      .await
      .map_err(|error| crate::wrap_backend_error(error, "Failed to inspect profile for rotation"))?
    {
      running.push(profile);
    }
  }

  let mut stopped = Vec::new();
  for profile in running {
    if let Err(error) = kill_browser_profile(app_handle.clone(), profile.clone()).await {
      for previous in stopped {
        let _ = launch_browser_profile(app_handle.clone(), previous, None).await;
      }
      return Err(error);
    }
    stopped.push(profile);
  }

  if stopped.is_empty() {
    return crate::vpn::pool::rotate_pool(pool_id).await;
  }

  // A live profile owns its own pool lease. Do not create a second always-on
  // pool worker here: that would consume a config and can deadlock a one-entry
  // pool. Removing any idle runtime lets the relaunched profiles allocate and
  // verify their replacement endpoints themselves.
  let _ = crate::vpn::pool::stop_pool(pool_id).await;
  let mut relaunch_error = None;
  for profile in stopped {
    if let Err(error) = launch_browser_profile(app_handle.clone(), profile, None).await {
      log::error!("Failed to relaunch profile after safe VPN rotation: {error}");
      relaunch_error.get_or_insert(error);
    }
  }
  if let Some(error) = relaunch_error {
    return Err(crate::backend_error_with_detail(
      "VPN_POOL_PROFILE_RELAUNCH_FAILED",
      error,
    ));
  }
  crate::vpn::pool::refresh_runtime_from_leases(pool_id).await
}

#[tauri::command]
pub fn check_browser_exists(browser_str: String, version: String) -> bool {
  // This is an alias for is_browser_downloaded to provide clearer semantics for auto-updates
  let runner = BrowserRunner::instance();
  runner
    .downloaded_browsers_registry
    .is_browser_downloaded(&browser_str, &version)
}

#[tauri::command]
pub async fn kill_browser_profile(
  app_handle: tauri::AppHandle,
  profile: BrowserProfile,
) -> Result<(), String> {
  log::info!(
    "Kill request received for profile: {} (ID: {})",
    profile.name,
    profile.id
  );
  let browser_runner = BrowserRunner::instance();

  match browser_runner
    .kill_browser_process(app_handle.clone(), &profile)
    .await
  {
    Ok(()) => {
      log::info!(
        "Successfully killed browser profile: {} (ID: {})",
        profile.name,
        profile.id
      );

      // Mark worker offline in WorkerRegistry
      let worker_id = format!("browser-profile:{}", profile.id);
      let _ = crate::worker::WORKER_REGISTRY
        .mark_worker_offline(&worker_id)
        .await;

      // Release team lock if applicable
      crate::team_lock::release_team_lock_if_needed(&profile).await;
      if profile
        .vpn_id
        .as_deref()
        .and_then(crate::vpn::pool::parse_pool_reference)
        .is_some()
      {
        let _ = crate::vpn::pool::release_profile_lease(&profile.id.to_string()).await;
      }

      // Notify sync scheduler that profile stopped (sync was queued at launch)
      if let Some(scheduler) = crate::sync::get_global_scheduler() {
        scheduler
          .mark_profile_stopped(&profile.id.to_string())
          .await;
      }

      // Auto-update non-running profiles and cleanup unused binaries
      let browser_for_update = profile.browser.clone();
      let app_handle_for_update = app_handle.clone();
      tauri::async_runtime::spawn(async move {
        let registry = crate::downloaded_browsers_registry::DownloadedBrowsersRegistry::instance();
        let mut versions = registry.get_downloaded_versions(&browser_for_update);
        if !versions.is_empty() {
          versions.sort_by(|a, b| crate::api_client::compare_versions(b, a));
          let latest_version = &versions[0];

          let auto_updater = crate::auto_updater::AutoUpdater::instance();
          match auto_updater
            .auto_update_profile_versions(
              &app_handle_for_update,
              &browser_for_update,
              latest_version,
            )
            .await
          {
            Ok(updated) => {
              if !updated.is_empty() {
                log::info!(
                  "Auto-updated {} profiles after stop: {:?}",
                  updated.len(),
                  updated
                );
              }
            }
            Err(e) => {
              log::error!("Failed to auto-update profile versions after stop: {e}");
            }
          }
        }

        match registry.cleanup_unused_binaries() {
          Ok(cleaned) => {
            if !cleaned.is_empty() {
              log::info!("Cleaned up unused binaries after stop: {:?}", cleaned);
            }
          }
          Err(e) => {
            log::error!("Failed to cleanup unused binaries after stop: {e}");
          }
        }
      });

      Ok(())
    }
    Err(e) => {
      log::info!("Failed to kill browser profile {}: {}", profile.name, e);

      // Emit a failure event to clear loading states in the frontend
      #[derive(serde::Serialize)]
      struct RunningChangedPayload {
        id: String,
        is_running: bool,
      }
      // On kill failure, we assume the process is still running
      let payload = RunningChangedPayload {
        id: profile.id.to_string(),
        is_running: true,
      };

      if let Err(e) = events::emit("profile-running-changed", &payload) {
        log::warn!("Warning: Failed to emit profile running changed event: {e}");
      }

      Err(format!("Failed to kill browser: {e}"))
    }
  }
}

#[tauri::command]
pub async fn open_url_with_profile(
  app_handle: tauri::AppHandle,
  profile_id: String,
  url: String,
) -> Result<(), String> {
  let browser_runner = BrowserRunner::instance();
  browser_runner
    .open_url_with_profile(app_handle, profile_id, url)
    .await
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
  use tokio::sync::Barrier;

  fn stale_binding_fixture(lifecycle: &str) -> ManagedTargetBindingLedger {
    ManagedTargetBindingLedger {
      profile_id: "profile".into(),
      managed_target_binding_id: "session".into(),
      last_known_target_id: "PENDING".into(),
      browser_pid: 123,
      cdp_port: 456,
      launch_generation: 789,
      managed_grok_page_url: "PENDING".into(),
      binding_created_at: 1,
      binding_version: 1,
      lifecycle: lifecycle.into(),
      executable: "chrome.exe".into(),
      user_data_dir: "profile".into(),
      expires_at: Some(2),
      candidates: vec![],
      previous_ledger: None,
    }
  }

  #[test]
  fn stale_binding_abort_without_previous_snapshot_is_removed() {
    let ledger = stale_binding_fixture("BINDING_REQUIRED");
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), None, false, false, false, false),
      StaleBindingReconcileAction::RemoveTemporary
    );
  }

  #[test]
  fn pending_binding_recovery_preserves_valid_receipt_and_defers_uncertain_identity() {
    assert_eq!(
      pending_binding_recovery_action(true, true, false),
      PendingBindingRecoveryAction::Preserve
    );
    assert_eq!(
      pending_binding_recovery_action(false, true, false),
      PendingBindingRecoveryAction::DeferIdentity
    );
  }

  #[test]
  fn pending_binding_recovery_rolls_back_only_missing_or_expired_receipts() {
    assert_eq!(
      pending_binding_recovery_action(true, false, false),
      PendingBindingRecoveryAction::Rollback
    );
    assert_eq!(
      pending_binding_recovery_action(true, true, true),
      PendingBindingRecoveryAction::Rollback
    );
  }

  #[test]
  fn stale_binding_abort_with_previous_snapshot_restores_committed() {
    let ledger = stale_binding_fixture("BINDING_REQUIRED");
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), None, false, false, true, false),
      StaleBindingReconcileAction::RestoreCommitted
    );
  }

  #[test]
  fn stale_binding_keeps_live_or_identity_uncertain_state() {
    let ledger = stale_binding_fixture("BINDING_REQUIRED");
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), Some(123), false, false, false, false),
      StaleBindingReconcileAction::Keep
    );
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), Some(999), false, false, false, false),
      StaleBindingReconcileAction::Keep
    );
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), None, true, false, false, false),
      StaleBindingReconcileAction::Keep
    );
    assert_eq!(
      stale_binding_reconcile_action(Some(&ledger), None, false, true, false, false),
      StaleBindingReconcileAction::Keep
    );
  }

  #[test]
  fn stale_binding_reconcile_is_idempotent_and_preserves_committed() {
    let committed = stale_binding_fixture("COMMITTED");
    assert_eq!(
      stale_binding_reconcile_action(Some(&committed), None, false, false, false, false),
      StaleBindingReconcileAction::Keep
    );
    assert_eq!(
      stale_binding_reconcile_action(None, None, false, false, false, false),
      StaleBindingReconcileAction::Keep
    );
  }

  #[test]
  fn orphan_marker_metadata_is_distinguished_from_durable_ledger() {
    let mut profile = BrowserProfile::default();
    assert!(!has_managed_marker_metadata(&profile));
    profile.managed_grok_marker_id = Some("orphan-cache".into());
    assert!(has_managed_marker_metadata(&profile));
    assert_eq!(
      stale_binding_reconcile_action(None, None, false, false, false, true),
      StaleBindingReconcileAction::ClearOrphanMetadata
    );
  }

  #[test]
  fn binding_prepare_response_uses_canonical_snake_case_identity_fields() {
    let response = TargetBindingPrepareResponse {
      binding_required: true,
      binding_session_id: "session".into(),
      browser_pid: 123,
      remote_debugging_port: 456,
      launch_generation: 789,
      candidate_count: 0,
      candidates: vec![],
    };
    let value = serde_json::to_value(response).unwrap();
    assert_eq!(value["binding_session_id"], "session");
    assert_eq!(value["browser_pid"], 123);
    assert_eq!(value["remote_debugging_port"], 456);
    assert_eq!(value["launch_generation"], 789);
    assert!(value.get("bindingSessionId").is_none());
  }

  #[test]
  fn pending_binding_response_round_trips_candidates_and_previous_ledger() {
    let candidate = TargetBindingCandidate {
      handle: "opaque-handle".into(),
      target_id: "target-1".into(),
      target_id_hash: "hash".into(),
      normalized_url: "https://grok.com/imagine".into(),
      hostname: "grok.com".into(),
      normalized_path: "/imagine".into(),
      title_hash: "title".into(),
    };
    let ledger = ManagedTargetBindingLedger {
      profile_id: "profile".into(),
      managed_target_binding_id: "session".into(),
      last_known_target_id: "PENDING".into(),
      browser_pid: 42,
      cdp_port: 9222,
      launch_generation: 7,
      managed_grok_page_url: "PENDING".into(),
      binding_created_at: 10,
      binding_version: 1,
      lifecycle: "BINDING_REQUIRED".into(),
      executable: "cft.exe".into(),
      user_data_dir: "profile-data".into(),
      expires_at: Some(100),
      candidates: vec![candidate],
      previous_ledger: Some(Box::new(stale_binding_fixture("COMMITTED"))),
    };
    let encoded = serde_json::to_vec(&ledger).unwrap();
    let decoded: ManagedTargetBindingLedger = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded.candidates.len(), 1);
    assert_eq!(decoded.candidates[0].handle, "opaque-handle");
    assert_eq!(
      decoded.previous_ledger.as_deref().unwrap().lifecycle,
      "COMMITTED"
    );
    let response = target_binding_prepare_response_from_ledger(&decoded);
    assert_eq!(response.binding_session_id, "session");
    assert_eq!(response.candidate_count, 1);
    assert_eq!(response.candidates[0].handle, "opaque-handle");
  }

  #[test]
  fn launch_receipt_preserves_spawn_identity() {
    let receipt = ChromiumLaunchReceipt {
      profile_id: "profile".into(),
      browser_pid: 42,
      cdp_port: Some(9222),
      launch_generation: 1234,
      executable: "chrome.exe".into(),
      user_data_dir: "profile-data".into(),
      spawned_at: 1234,
    };
    let value = serde_json::to_value(&receipt).unwrap();
    assert_eq!(value["profileId"], "profile");
    assert_eq!(value["browserPid"], 42);
    assert_eq!(value["cdpPort"], 9222);
    assert_eq!(value["launchGeneration"], 1234);
  }

  #[test]
  fn durable_receipt_replace_keeps_complete_json() {
    let directory = tempfile::tempdir().unwrap();
    let destination = directory.path().join("pending.json");
    let temporary = directory.path().join("pending.json.tmp");
    std::fs::write(&destination, br#"{"old":true}"#).unwrap();
    std::fs::write(&temporary, br#"{"new":true}"#).unwrap();
    atomic_replace_file(&temporary, &destination).unwrap();
    let value: serde_json::Value =
      serde_json::from_slice(&std::fs::read(&destination).unwrap()).unwrap();
    assert_eq!(value["new"], true);
    assert!(!temporary.exists());
  }

  #[test]
  fn post_spawn_failure_contract_reports_process_and_rollback() {
    let value: serde_json::Value = serde_json::from_str(&target_binding_prepare_failure_json(
      "TARGET_BINDING_CDP_READINESS_FAILED",
      "CDP_READINESS",
      "CDP did not become ready",
      true,
      true,
      false,
    ))
    .unwrap();
    assert_eq!(value["code"], "TARGET_BINDING_CDP_READINESS_FAILED");
    assert_eq!(value["stage"], "CDP_READINESS");
    assert_eq!(value["processSpawned"], true);
    assert_eq!(value["rollbackAttempted"], true);
    assert_eq!(value["rollbackSucceeded"], false);
    assert_ne!(value["stage"], "UNKNOWN");
  }

  #[tokio::test]
  async fn profile_launch_lock_serializes_only_the_same_profile() {
    let profile = format!("launch-lock-{}", uuid::Uuid::new_v4());
    let other_profile = format!("launch-lock-{}", uuid::Uuid::new_v4());
    let first = lock_profile_launch(&profile).await;

    assert!(tokio::time::timeout(
      Duration::from_millis(100),
      lock_profile_launch(&other_profile)
    )
    .await
    .is_ok());
    assert!(
      tokio::time::timeout(Duration::from_millis(100), lock_profile_launch(&profile))
        .await
        .is_err()
    );

    drop(first);
    assert!(
      tokio::time::timeout(Duration::from_millis(100), lock_profile_launch(&profile))
        .await
        .is_ok()
    );
  }

  #[tokio::test]
  async fn cold_start_only_launches_once_when_two_requests_race() {
    let profile_id = format!("cold-start-only-{}", uuid::Uuid::new_v4());
    let browser_alive = Arc::new(AtomicBool::new(false));
    let launch_count = Arc::new(AtomicUsize::new(0));
    let navigation_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(2));
    let make_request = |profile_id: String,
                        barrier: Arc<Barrier>,
                        browser_alive: Arc<AtomicBool>,
                        launch_count: Arc<AtomicUsize>,
                        navigation_count: Arc<AtomicUsize>| async move {
      barrier.wait().await;
      let check_alive = browser_alive.clone();
      launch_with_url_policy(
        &profile_id,
        LaunchUrlPolicy::ColdStartOnly("https://grok.com/imagine".to_string()),
        move || {
          let alive = check_alive.load(Ordering::SeqCst);
          async move { Ok(alive) }
        },
        move |url| {
          let browser_alive = browser_alive.clone();
          let launch_count = launch_count.clone();
          let navigation_count = navigation_count.clone();
          async move {
            if url.is_some() {
              launch_count.fetch_add(1, Ordering::SeqCst);
              navigation_count.fetch_add(1, Ordering::SeqCst);
              browser_alive.store(true, Ordering::SeqCst);
              Ok(false)
            } else {
              Ok(true)
            }
          }
        },
      )
      .await
    };
    let first = make_request(
      profile_id.clone(),
      barrier.clone(),
      browser_alive.clone(),
      launch_count.clone(),
      navigation_count.clone(),
    );
    let second = make_request(
      profile_id,
      barrier,
      browser_alive.clone(),
      launch_count.clone(),
      navigation_count.clone(),
    );
    let result = tokio::time::timeout(Duration::from_secs(5), async {
      tokio::join!(first, second)
    })
    .await
    .expect("ColdStartOnly requests must not deadlock");
    assert_eq!(
      result.0.unwrap().1 as usize + result.1.unwrap().1 as usize,
      1
    );
    assert_eq!(launch_count.load(Ordering::SeqCst), 1);
    assert_eq!(navigation_count.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn cold_start_only_reuses_live_browser_without_launch_or_navigation() {
    let browser_alive = Arc::new(AtomicBool::new(true));
    let launch_count = Arc::new(AtomicUsize::new(0));
    let navigation_count = Arc::new(AtomicUsize::new(0));
    let result = launch_with_url_policy(
      &format!("cold-start-live-{}", uuid::Uuid::new_v4()),
      LaunchUrlPolicy::ColdStartOnly("https://grok.com/imagine".to_string()),
      move || {
        let alive = browser_alive.load(Ordering::SeqCst);
        async move { Ok(alive) }
      },
      move |url| async move {
        assert!(url.is_none());
        Ok(true)
      },
    )
    .await
    .expect("live browser reuse should succeed");
    assert!(result.1);
    assert_eq!(launch_count.load(Ordering::SeqCst), 0);
    assert_eq!(navigation_count.load(Ordering::SeqCst), 0);
  }

  #[tokio::test]
  async fn always_open_reports_reuse_without_suppressing_navigation() {
    let launch_url = "https://example.com".to_string();
    let result = launch_with_url_policy(
      &format!("always-open-{}", uuid::Uuid::new_v4()),
      LaunchUrlPolicy::AlwaysOpen(Some(launch_url.clone())),
      || async { Ok(true) },
      move |url| async move {
        assert_eq!(url.as_deref(), Some(launch_url.as_str()));
        Ok(())
      },
    )
    .await
    .expect("generic AlwaysOpen launch should succeed");
    assert!(result.1, "reused must reflect the live browser state");
  }

  #[test]
  fn floword_cold_start_does_not_put_grok_url_on_chromium_command_line() {
    assert_eq!(
      sanitize_chromium_launch_url(Some("https://grok.com/imagine".to_string()), true),
      None
    );
    assert_eq!(
      sanitize_chromium_launch_url(Some("https://example.com".to_string()), true).as_deref(),
      Some("https://example.com")
    );
    assert_eq!(
      sanitize_chromium_launch_url(Some("https://grok.com/imagine".to_string()), false).as_deref(),
      Some("https://grok.com/imagine")
    );
  }

  #[test]
  fn marker_checkpoint_serialization_redacts_raw_marker() {
    let raw_marker = "marker-secret-token";
    let checkpoint = MarkerLifecycleCheckpoint {
      checkpoint: "MARKER_WRITE_AFTER".into(),
      profile_id: "profile".into(),
      generation: Some(7),
      target_id_hash: Some(target_id_hash("target")),
      marker_hash: Some(marker_hash(raw_marker)),
      marker_present: true,
      fragment_matched: true,
      window_name_matched: true,
      navigation_entry_matched: true,
      normalized_url: Some("https://grok.com/imagine".into()),
      document_lifecycle: "active".into(),
      elapsed_ms: 1,
    };
    let encoded = serde_json::to_string(&checkpoint).unwrap();
    assert!(!encoded.contains(raw_marker));
    assert!(encoded.contains("markerHash"));
    assert!(!encoded.contains("floword-managed="));
  }

  #[test]
  fn marker_session_scan_distinguishes_exact_token_from_prefix() {
    assert!(bytes_contain_exact_marker(
      b"floword-managed=marker-secret-token",
      "marker-secret-token"
    ));
    assert!(!bytes_contain_exact_marker(
      b"floword-managed=other-token",
      "marker-secret-token"
    ));
    assert!(!bytes_contain_exact_marker(
      b"floword-managed=",
      "marker-secret-token"
    ));
  }

  #[test]
  fn grok_url_and_blank_target_classification_is_strict() {
    assert!(is_grok_url("https://grok.com/imagine"));
    assert!(is_grok_url("https://sub.grok.com/imagine"));
    assert!(!is_grok_url("https://example.com/grok.com/imagine"));
    assert!(is_blank_url(""));
    assert!(is_blank_url("about:blank"));
    assert!(!is_blank_url("https://grok.com/imagine"));
  }

  #[test]
  fn stale_managed_mapping_allows_single_startup_blank_recovery() {
    let mut profile = BrowserProfile {
      process_id: Some(38720),
      last_launch: Some(1788099067),
      managed_grok_browser_pid: Some(28800),
      managed_grok_cdp_port: Some(52257),
      managed_grok_launch_generation: Some(1788073835),
      ..BrowserProfile::default()
    };
    assert!(managed_mapping_is_stale(&profile, 55182));

    profile.managed_grok_browser_pid = profile.process_id;
    profile.managed_grok_cdp_port = Some(55182);
    profile.managed_grok_launch_generation = profile.last_launch;
    assert!(!managed_mapping_is_stale(&profile, 55182));
  }

  fn migration_fixture() -> (BrowserProfile, StartupGrokMigrationHint, Vec<CdpPageTarget>) {
    let profile_id = uuid::Uuid::new_v4();
    let profile = BrowserProfile {
      id: profile_id,
      name: "migration".into(),
      browser: "chromium".into(),
      version: "151".into(),
      process_id: Some(28800),
      last_launch: Some(1788073835),
      ..BrowserProfile::default()
    };
    let hint = StartupGrokMigrationHint {
      profile_id,
      target_id: "managed-target".into(),
      browser_pid: 28800,
      cdp_port: 52257,
      launch_generation: 1788073835,
    };
    let pages = vec![
      CdpPageTarget {
        id: "managed-target".into(),
        url: "https://grok.com/imagine".into(),
        websocket: "ws://managed".into(),
        title: "Imagine".into(),
      },
      CdpPageTarget {
        id: "unowned-target".into(),
        url: "https://grok.com/imagine".into(),
        websocket: "ws://unowned".into(),
        title: "Imagine".into(),
      },
    ];
    (profile, hint, pages)
  }

  #[test]
  fn startup_migration_selects_only_the_exact_mapped_target() {
    let (profile, hint, pages) = migration_fixture();
    let selected =
      select_startup_migration_target(&profile, &hint, &pages, &HashMap::new(), "stable-marker")
        .unwrap();
    assert_eq!(selected.0.id, "managed-target");
    assert_eq!(selected.1, "EXACT_CURRENT_GENERATION_MAPPING");
    assert_eq!(pages.len(), 2, "planning must not mutate the target set");
  }

  #[test]
  fn startup_migration_uses_one_matching_marker_when_target_id_changed() {
    let (profile, mut hint, pages) = migration_fixture();
    hint.target_id = "stale-target".into();
    let marker = ManagedGrokMarker {
      version: 1,
      marker_id: "stable-marker".into(),
      profile_id: profile.id.to_string(),
      browser_pid: hint.browser_pid,
      launch_generation: hint.launch_generation,
      transaction_id: "transaction".into(),
    };
    let mut markers = HashMap::new();
    markers.insert("managed-target".into(), Some(marker));
    let selected =
      select_startup_migration_target(&profile, &hint, &pages, &markers, "stable-marker")
        .expect("a single matching marker is authoritative");
    assert_eq!(selected.0.id, "managed-target");
    assert_eq!(selected.1, "DURABLE_MARKER_MATCH");
  }

  #[test]
  fn startup_migration_rejects_duplicate_matching_markers() {
    let (profile, mut hint, pages) = migration_fixture();
    hint.target_id = "stale-target".into();
    let marker = |transaction_id: &str| ManagedGrokMarker {
      version: 1,
      marker_id: "stable-marker".into(),
      profile_id: profile.id.to_string(),
      browser_pid: hint.browser_pid,
      launch_generation: hint.launch_generation,
      transaction_id: transaction_id.into(),
    };
    let mut markers = HashMap::new();
    markers.insert("managed-target".into(), Some(marker("one")));
    markers.insert("unowned-target".into(), Some(marker("two")));
    assert_eq!(
      select_startup_migration_target(&profile, &hint, &pages, &markers, "stable-marker")
        .unwrap_err(),
      "DUPLICATE_MANAGED_TARGET_MARKER"
    );
  }

  #[test]
  fn binding_marker_requires_durable_profile_ledger() {
    let (profile, _, _) = migration_fixture();
    let marker = ManagedGrokMarker {
      version: 1,
      marker_id: "binding-token".into(),
      profile_id: profile.id.to_string(),
      browser_pid: 999,
      launch_generation: 1,
      transaction_id: "transaction".into(),
    };
    let ledger = ManagedTargetBindingLedger {
      profile_id: profile.id.to_string(),
      managed_target_binding_id: "binding-token".into(),
      last_known_target_id: "restored-target".into(),
      browser_pid: 28800,
      cdp_port: 52257,
      launch_generation: 1788073835,
      managed_grok_page_url: "https://grok.com/imagine".into(),
      binding_created_at: 1,
      binding_version: 1,
      lifecycle: "COMMITTED".into(),
      executable: "chrome.exe".into(),
      user_data_dir: "profile".into(),
      expires_at: None,
      candidates: vec![],
      previous_ledger: None,
    };
    assert!(marker_matches_binding_ledger(
      &marker,
      &profile,
      Some(&ledger)
    ));
    assert!(!marker_matches_binding_ledger(&marker, &profile, None));
    let mut sibling = ledger.clone();
    sibling.profile_id = uuid::Uuid::new_v4().to_string();
    assert!(!marker_matches_binding_ledger(
      &marker,
      &profile,
      Some(&sibling)
    ));
  }

  #[test]
  fn managed_marker_fragment_is_opaque_and_round_trips() {
    let marker_id = new_opaque_marker_id();
    assert!(marker_id.len() >= 32);
    let url = format!(
      "https://grok.com/imagine{}",
      managed_marker_fragment(&marker_id)
    );
    assert_eq!(marker_id_from_fragment(&url), Some(marker_id));
    assert_eq!(normalized_public_grok_url(&url), "https://grok.com/imagine");
  }

  #[test]
  fn marker_fragment_rejects_non_managed_or_unsafe_values() {
    assert_eq!(
      marker_id_from_fragment("https://grok.com/imagine#other=value"),
      None
    );
    assert_eq!(
      marker_id_from_fragment("https://grok.com/imagine#floword-managed=a%2Fb"),
      None
    );
    assert_eq!(
      marker_id_from_fragment("https://grok.com/imagine#floword-managed="),
      None
    );
  }

  #[test]
  fn navigation_history_marker_must_match_current_entry() {
    let value = serde_json::json!({
      "result": {
        "currentIndex": 1,
        "entries": [
          {"url": "https://grok.com/imagine#floword-managed=old"},
          {"url": "https://grok.com/imagine#floword-managed=new"}
        ]
      }
    });
    assert!(navigation_history_current_entry_matches(&value, "new"));
    assert!(!navigation_history_current_entry_matches(&value, "old"));
    let mut previous = value.clone();
    previous["result"]["currentIndex"] = serde_json::json!(0);
    assert!(!navigation_history_current_entry_matches(&previous, "new"));
  }

  #[test]
  fn marker_reconciliation_allows_only_managed_fragment_replacement() {
    let managed = url::Url::parse("https://grok.com/imagine#floword-managed=old").unwrap();
    assert!(managed
      .fragment()
      .unwrap()
      .starts_with(MANAGED_GROK_FRAGMENT_PREFIX));
    let foreign = url::Url::parse("https://grok.com/imagine#subscribe").unwrap();
    assert!(!foreign
      .fragment()
      .unwrap()
      .starts_with(MANAGED_GROK_FRAGMENT_PREFIX));
  }

  #[test]
  fn raw_target_id_is_not_authoritative_across_generation() {
    let (mut profile, hint, _) = migration_fixture();
    profile.managed_grok_target_id = Some(hint.target_id.clone());
    profile.managed_grok_launch_generation = Some(hint.launch_generation + 1);
    assert!(managed_mapping_is_stale(&profile, hint.cdp_port));
  }

  #[test]
  fn startup_migration_adopts_single_live_target_when_exact_is_stale() {
    let (profile, mut hint, mut pages) = migration_fixture();
    hint.target_id = "stale-target".into();
    pages.remove(1);
    let selected =
      select_startup_migration_target(&profile, &hint, &pages, &HashMap::new(), "stable-marker")
        .unwrap();
    assert_eq!(selected.0.id, "managed-target");
    assert_eq!(selected.1, "ADOPTED_SINGLE_EXISTING");
  }

  #[test]
  fn startup_migration_rejects_multiple_or_missing_targets_when_exact_is_stale() {
    let (profile, mut hint, pages) = migration_fixture();
    hint.target_id = "stale-target".into();
    assert_eq!(
      select_startup_migration_target(&profile, &hint, &pages, &HashMap::new(), "stable-marker")
        .unwrap_err(),
      "AMBIGUOUS_GROK_TAB"
    );
    assert_eq!(
      select_startup_migration_target(&profile, &hint, &[], &HashMap::new(), "stable-marker")
        .unwrap_err(),
      "GROK_TAB_NOT_FOUND"
    );
  }

  #[test]
  fn startup_migration_rejects_foreign_marker() {
    let (profile, mut hint, mut pages) = migration_fixture();
    hint.target_id = "stale-target".into();
    pages.remove(1);
    let mut markers = HashMap::new();
    markers.insert(
      "managed-target".into(),
      Some(ManagedGrokMarker {
        version: 1,
        marker_id: "other-marker".into(),
        profile_id: uuid::Uuid::new_v4().to_string(),
        browser_pid: 28800,
        launch_generation: 1788073835,
        transaction_id: "other-transaction".into(),
      }),
    );
    assert_eq!(
      select_startup_migration_target(&profile, &hint, &pages, &markers, "stable-marker")
        .unwrap_err(),
      "GROK_TARGET_MARKER_CONFLICT"
    );
  }

  #[test]
  fn startup_migration_rejects_stale_pid_and_generation() {
    let (profile, hint, _) = migration_fixture();
    let mut stale_pid = hint.clone();
    stale_pid.browser_pid += 1;
    assert_eq!(
      validate_startup_migration_identity(&profile, &stale_pid).unwrap_err(),
      "GROK_BROWSER_IDENTITY_CHANGED"
    );
    let mut stale_generation = hint.clone();
    stale_generation.launch_generation += 1;
    assert_eq!(
      validate_startup_migration_identity(&profile, &stale_generation).unwrap_err(),
      "GROK_BROWSER_IDENTITY_CHANGED"
    );
  }

  #[test]
  fn startup_migration_does_not_adopt_different_snapshots() {
    let (_, _, first) = migration_fixture();
    let mut second = first.clone();
    second.pop();
    assert_ne!(cdp_page_fingerprint(&first), cdp_page_fingerprint(&second));
  }

  #[test]
  fn startup_migration_recovers_from_verified_marker_after_persist_failure() {
    let (mut profile, mut hint, mut pages) = migration_fixture();
    profile.managed_grok_marker_id = Some("stable-marker".into());
    hint.target_id = "stale-target".into();
    pages.remove(1);
    let marker = ManagedGrokMarker {
      version: 1,
      marker_id: "stable-marker".into(),
      profile_id: profile.id.to_string(),
      browser_pid: 28800,
      launch_generation: 1788073835,
      transaction_id: "verified-write".into(),
    };
    let mut markers = HashMap::new();
    markers.insert("managed-target".into(), Some(marker));
    let selected =
      select_startup_migration_target(&profile, &hint, &pages, &markers, "stable-marker").unwrap();
    assert_eq!(selected.0.id, "managed-target");
    assert_eq!(selected.1, "DURABLE_MARKER_MATCH");
  }

  #[test]
  fn startup_migration_reuses_existing_marker_id() {
    let (mut profile, _, _) = migration_fixture();
    profile.managed_grok_marker_id = Some("stable-marker".into());
    let (first, first_created) = startup_migration_marker_id(&profile);
    let (second, second_created) = startup_migration_marker_id(&profile);
    assert_eq!(first, "stable-marker");
    assert_eq!(first, second);
    assert!(!first_created);
    assert!(!second_created);
  }

  #[test]
  fn startup_migration_creates_no_target_mutations() {
    let result = StartupGrokMigrationResult {
      profile_id: uuid::Uuid::new_v4().to_string(),
      target_id_hash: target_id_hash("managed-target"),
      marker_written: true,
      marker_verified: true,
      target_count_before: 2,
      target_count_after: 2,
      selection_path: "ADOPTED_SINGLE_EXISTING".into(),
      created_target_count: 0,
      closed_target_count: 0,
      navigated_target_count: 0,
      reloaded_target_count: 0,
    };
    assert_eq!(result.target_count_before, result.target_count_after);
    assert_eq!(result.created_target_count, 0);
    assert_eq!(result.closed_target_count, 0);
    assert_eq!(result.navigated_target_count, 0);
    assert_eq!(result.reloaded_target_count, 0);
  }

  #[tokio::test]
  async fn vpn_pool_rotation_lock_serializes_only_the_same_pool() {
    let pool = format!("rotation-lock-{}", uuid::Uuid::new_v4());
    let other_pool = format!("rotation-lock-{}", uuid::Uuid::new_v4());
    let first = lock_vpn_pool_rotation(&pool).await;

    assert!(tokio::time::timeout(
      Duration::from_millis(100),
      lock_vpn_pool_rotation(&other_pool)
    )
    .await
    .is_ok());
    assert!(
      tokio::time::timeout(Duration::from_millis(100), lock_vpn_pool_rotation(&pool))
        .await
        .is_err()
    );

    drop(first);
    assert!(
      tokio::time::timeout(Duration::from_millis(100), lock_vpn_pool_rotation(&pool))
        .await
        .is_ok()
    );
  }
}

// Global singleton instance
lazy_static::lazy_static! {
  static ref BROWSER_RUNNER: BrowserRunner = BrowserRunner::new();
}
