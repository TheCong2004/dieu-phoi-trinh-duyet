use super::{provider::PROVIDER_STORE, VpnProviderKind, VPN_STORAGE};
use crate::{proxy_manager::now_secs, vpn_worker_runner};
use serde::{Deserialize, Serialize};
use std::{
  collections::{HashMap, HashSet},
  fs,
  path::PathBuf,
  sync::atomic::{AtomicU64, Ordering},
};
use tokio::sync::{Mutex, Notify, Semaphore};
use uuid::Uuid;

const DEFAULT_GLOBAL_CAP: usize = 20;
const MAX_WAIT_SECONDS: u64 = 300;
pub const POOL_REFERENCE_PREFIX: &str = "pool:";

fn has_global_capacity(active: usize) -> bool {
  active < DEFAULT_GLOBAL_CAP
}

fn has_provider_capacity(active: usize, cap: u32) -> bool {
  active < cap as usize
}

fn strategy_index(
  strategy: PoolSelectionStrategy,
  config_ids: &[String],
  cursor: &mut usize,
  last_used: &HashMap<String, u64>,
) -> usize {
  match strategy {
    PoolSelectionStrategy::RoundRobin => {
      let index = *cursor % config_ids.len();
      *cursor = cursor.wrapping_add(1);
      index
    }
    PoolSelectionStrategy::LeastRecentlyUsed => config_ids
      .iter()
      .enumerate()
      .min_by_key(|(_, id)| last_used.get(*id).copied().unwrap_or(0))
      .map(|(index, _)| index)
      .unwrap_or_default(),
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PoolSelectionStrategy {
  RoundRobin,
  LeastRecentlyUsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RotationMode {
  Safe,
  Hot,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct VpnPool {
  pub id: String,
  pub name: String,
  pub provider_filter: Vec<VpnProviderKind>,
  pub country: Option<String>,
  pub config_ids: Vec<String>,
  pub rotation_enabled: bool,
  pub rotation_interval_sec: Option<u64>,
  pub rotation_mode: RotationMode,
  pub strategy: PoolSelectionStrategy,
  pub enabled: bool,
  pub created_at: i64,
  pub updated_at: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VpnPoolStatus {
  Stopped,
  Starting,
  Connected,
  Rotating,
  Degraded,
  Error,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, utoipa::ToSchema)]
pub struct VpnHealth {
  pub latency_ms: Option<u64>,
  pub verified_at: Option<i64>,
  pub consecutive_failures: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct VpnPoolRuntime {
  pub pool_id: String,
  pub active_config_id: Option<String>,
  pub status: VpnPoolStatus,
  pub exit_ip: Option<String>,
  pub exit_country: Option<String>,
  pub next_rotation_at: Option<i64>,
  pub health: VpnHealth,
  pub last_error_code: Option<String>,
  pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum LeaseStatus {
  Provisioning,
  Active,
  Releasing,
  Failed,
  Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum ProxyProtocol {
  Socks5,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct VpnLease {
  pub id: String,
  pub pool_id: Option<String>,
  pub config_id: String,
  pub provider: VpnProviderKind,
  pub country: Option<String>,
  pub profile_id: Option<String>,
  pub local_host: String,
  pub local_port: u16,
  pub protocol: ProxyProtocol,
  pub exit_ip: Option<String>,
  pub created_at: i64,
  pub expires_at: Option<i64>,
  pub status: LeaseStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AcquireVpnLeaseRequest {
  pub pool_id: Option<String>,
  pub country: Option<String>,
  #[serde(default)]
  pub providers: Vec<VpnProviderKind>,
  pub profile_id: Option<String>,
  pub ttl_seconds: Option<u64>,
  pub protocol: Option<ProxyProtocol>,
  #[serde(default)]
  pub wait_when_full: bool,
  pub max_wait_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct CreateVpnPoolRequest {
  pub name: String,
  #[serde(default)]
  pub provider_filter: Vec<VpnProviderKind>,
  pub country: Option<String>,
  #[serde(default)]
  pub config_ids: Vec<String>,
  #[serde(default)]
  pub rotation_enabled: bool,
  pub rotation_interval_sec: Option<u64>,
  pub strategy: PoolSelectionStrategy,
  #[serde(default = "default_rotation_mode")]
  pub rotation_mode: RotationMode,
  #[serde(default = "default_enabled")]
  pub enabled: bool,
}

fn default_rotation_mode() -> RotationMode {
  RotationMode::Safe
}
fn default_enabled() -> bool {
  true
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct PoolStorageData {
  #[serde(default)]
  pools: Vec<VpnPool>,
}

#[derive(Debug, Clone)]
struct Reservation {
  config_id: String,
  provider: VpnProviderKind,
  pool_id: Option<String>,
}

struct PoolState {
  path: PathBuf,
  pools: Vec<VpnPool>,
  runtimes: HashMap<String, VpnPoolRuntime>,
  runtime_providers: HashMap<String, VpnProviderKind>,
  leases: HashMap<String, VpnLease>,
  reservations: HashMap<String, Reservation>,
  last_used: HashMap<String, u64>,
  round_robin: HashMap<String, usize>,
  use_sequence: u64,
}

impl PoolState {
  fn new() -> Self {
    let path = crate::app_dirs::vpn_dir().join("vpn_pools.json");
    let pools = fs::read_to_string(&path)
      .ok()
      .and_then(|content| serde_json::from_str::<PoolStorageData>(&content).ok())
      .unwrap_or_default()
      .pools;
    Self {
      path,
      pools,
      runtimes: HashMap::new(),
      runtime_providers: HashMap::new(),
      leases: HashMap::new(),
      reservations: HashMap::new(),
      last_used: HashMap::new(),
      round_robin: HashMap::new(),
      use_sequence: 0,
    }
  }

  #[cfg(test)]
  fn with_path(path: PathBuf) -> Self {
    Self {
      path,
      pools: Vec::new(),
      runtimes: HashMap::new(),
      runtime_providers: HashMap::new(),
      leases: HashMap::new(),
      reservations: HashMap::new(),
      last_used: HashMap::new(),
      round_robin: HashMap::new(),
      use_sequence: 0,
    }
  }

  fn save(&self) -> Result<(), String> {
    if let Some(parent) = self.path.parent() {
      fs::create_dir_all(parent).map_err(|_| error("VPN_POOL_STORAGE_FAILED"))?;
    }
    let content = serde_json::to_string_pretty(&PoolStorageData {
      pools: self.pools.clone(),
    })
    .map_err(|_| error("VPN_POOL_STORAGE_FAILED"))?;
    fs::write(&self.path, content).map_err(|_| error("VPN_POOL_STORAGE_FAILED"))?;
    crate::app_dirs::restrict_to_owner(&self.path);
    Ok(())
  }

  fn active_count(&self) -> usize {
    self
      .leases
      .values()
      .filter(|lease| {
        matches!(
          lease.status,
          LeaseStatus::Provisioning | LeaseStatus::Active
        )
      })
      .count()
      + self.reservations.len()
      + self.runtime_providers.len()
  }

  fn provider_count(&self, provider: VpnProviderKind) -> usize {
    self
      .leases
      .values()
      .filter(|lease| {
        lease.provider == provider
          && matches!(
            lease.status,
            LeaseStatus::Provisioning | LeaseStatus::Active
          )
      })
      .count()
      + self
        .reservations
        .values()
        .filter(|reservation| reservation.provider == provider)
        .count()
      + self
        .runtime_providers
        .values()
        .filter(|runtime_provider| **runtime_provider == provider)
        .count()
  }

  fn config_busy(&self, config_id: &str) -> bool {
    self.leases.values().any(|lease| {
      lease.config_id == config_id
        && matches!(
          lease.status,
          LeaseStatus::Provisioning | LeaseStatus::Active | LeaseStatus::Releasing
        )
    }) || self
      .reservations
      .values()
      .any(|reservation| reservation.config_id == config_id)
      || self.runtimes.values().any(|runtime| {
        runtime.active_config_id.as_deref() == Some(config_id)
          && runtime.status != VpnPoolStatus::Stopped
      })
  }

  fn select_candidate(
    &mut self,
    request: &AcquireVpnLeaseRequest,
    excluded_config_ids: &HashSet<String>,
  ) -> Result<(String, VpnProviderKind, Option<String>), String> {
    if !has_global_capacity(self.active_count()) {
      return Err(error("VPN_LEASE_CAPACITY"));
    }
    let metadata = PROVIDER_STORE
      .lock()
      .map_err(|_| error("INTERNAL_ERROR"))?
      .metadata()
      .map_err(|_| error("INTERNAL_ERROR"))?;
    let pool = request
      .pool_id
      .as_ref()
      .map(|id| {
        self
          .pools
          .iter()
          .find(|pool| &pool.id == id)
          .cloned()
          .ok_or_else(|| error("VPN_POOL_NOT_FOUND"))
      })
      .transpose()?;
    let strategy = pool
      .as_ref()
      .map_or(PoolSelectionStrategy::LeastRecentlyUsed, |pool| {
        pool.strategy
      });
    let pool_key = pool
      .as_ref()
      .map_or_else(|| "__global__".to_string(), |pool| pool.id.clone());
    let mut candidates = Vec::new();
    for item in metadata {
      if excluded_config_ids.contains(&item.config_id) || self.config_busy(&item.config_id) {
        continue;
      }
      if let Some(pool) = &pool {
        if !pool.enabled
          || (!pool.config_ids.is_empty() && !pool.config_ids.contains(&item.config_id))
        {
          continue;
        }
        if !pool.provider_filter.is_empty() && !pool.provider_filter.contains(&item.provider) {
          continue;
        }
        if pool.country.as_ref().is_some_and(|country| {
          item
            .country
            .as_ref()
            .is_none_or(|value| !value.eq_ignore_ascii_case(country))
        }) {
          continue;
        }
      }
      if !request.providers.is_empty() && !request.providers.contains(&item.provider) {
        continue;
      }
      if request.country.as_ref().is_some_and(|country| {
        item
          .country
          .as_ref()
          .is_none_or(|value| !value.eq_ignore_ascii_case(country))
      }) {
        continue;
      }
      let account = PROVIDER_STORE
        .lock()
        .map_err(|_| error("INTERNAL_ERROR"))?
        .list_accounts()
        .map_err(|_| error("INTERNAL_ERROR"))?
        .into_iter()
        .find(|account| account.id == item.account_id)
        .ok_or_else(|| error("VPN_PROVIDER_ACCOUNT_NOT_FOUND"))?;
      if account.status != super::provider::ProviderAccountStatus::Ok
        || !has_provider_capacity(self.provider_count(item.provider), account.connection_cap)
      {
        continue;
      }
      candidates.push(item);
    }
    if candidates.is_empty() {
      return Err(error("VPN_LEASE_NO_CONFIG"));
    }
    let least_provider_load = candidates
      .iter()
      .map(|item| self.provider_count(item.provider))
      .min()
      .unwrap_or_default();
    candidates.retain(|item| self.provider_count(item.provider) == least_provider_load);
    let config_ids = candidates
      .iter()
      .map(|item| item.config_id.clone())
      .collect::<Vec<_>>();
    let cursor = self.round_robin.entry(pool_key).or_default();
    let selected_index = strategy_index(strategy, &config_ids, cursor, &self.last_used);
    let selected = candidates.swap_remove(selected_index);
    self.use_sequence = self.use_sequence.wrapping_add(1);
    self
      .last_used
      .insert(selected.config_id.clone(), self.use_sequence);
    Ok((selected.config_id, selected.provider, selected.country))
  }
}

static STATE: once_cell::sync::Lazy<Mutex<PoolState>> =
  once_cell::sync::Lazy::new(|| Mutex::new(PoolState::new()));
static CAPACITY_NOTIFY: once_cell::sync::Lazy<Notify> = once_cell::sync::Lazy::new(Notify::new);
static GENERATION: AtomicU64 = AtomicU64::new(1);
static VERIFY_LIMIT: once_cell::sync::Lazy<Semaphore> =
  once_cell::sync::Lazy::new(|| Semaphore::new(4));
static APP_HANDLE: once_cell::sync::OnceCell<tauri::AppHandle> = once_cell::sync::OnceCell::new();

pub fn set_app_handle(app_handle: tauri::AppHandle) {
  let _ = APP_HANDLE.set(app_handle);
}

fn error(code: &str) -> String {
  serde_json::json!({ "code": code }).to_string()
}

fn classify_verify_error(detail: &str) -> &'static str {
  let detail = detail.to_ascii_lowercase();
  if detail.contains("certificate") || detail.contains("tls") || detail.contains("ssl") {
    "VPN_VERIFY_TLS_FAILED"
  } else if detail.contains("not an ip address") || detail.contains("invalid") {
    "VPN_VERIFY_INVALID_RESPONSE"
  } else {
    "VPN_VERIFY_PROXY_FAILED"
  }
}

fn normalized_country(country: Option<&str>) -> Result<Option<String>, String> {
  country
    .map(str::trim)
    .filter(|value| !value.is_empty())
    .map(|value| {
      if value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_alphabetic()) {
        Ok(value.to_ascii_uppercase())
      } else {
        Err(error("VPN_POOL_COUNTRY_INVALID"))
      }
    })
    .transpose()
}

pub async fn list_pools() -> Vec<VpnPool> {
  STATE.lock().await.pools.clone()
}
pub async fn list_runtimes() -> Vec<VpnPoolRuntime> {
  STATE.lock().await.runtimes.values().cloned().collect()
}
pub async fn list_leases() -> Vec<VpnLease> {
  STATE.lock().await.leases.values().cloned().collect()
}

pub fn parse_pool_reference(value: &str) -> Option<&str> {
  value
    .strip_prefix(POOL_REFERENCE_PREFIX)
    .filter(|id| !id.is_empty())
}

pub async fn acquire_profile_lease(pool_id: &str, profile_id: &str) -> Result<VpnLease, String> {
  acquire_lease(AcquireVpnLeaseRequest {
    pool_id: Some(pool_id.to_string()),
    country: None,
    providers: Vec::new(),
    profile_id: Some(profile_id.to_string()),
    ttl_seconds: None,
    protocol: Some(ProxyProtocol::Socks5),
    wait_when_full: true,
    max_wait_seconds: Some(60),
  })
  .await
}

pub async fn release_profile_lease(profile_id: &str) -> Result<bool, String> {
  let lease_id = STATE
    .lock()
    .await
    .leases
    .values()
    .find(|lease| lease.profile_id.as_deref() == Some(profile_id))
    .map(|lease| lease.id.clone());
  match lease_id {
    Some(id) => release_lease(&id).await,
    None => Ok(false),
  }
}

pub fn monitor_profile_lease(profile_id: String, process_id: u32) {
  tokio::spawn(async move {
    loop {
      tokio::time::sleep(std::time::Duration::from_secs(2)).await;
      if !crate::proxy_storage::is_process_running(process_id) {
        let _ = release_profile_lease(&profile_id).await;
        break;
      }
    }
  });
}

fn validate_pool_request(request: &CreateVpnPoolRequest) -> Result<(), String> {
  if request.name.trim().is_empty() {
    return Err(error("VPN_POOL_NAME_EMPTY"));
  }
  if request.rotation_enabled
    && request
      .rotation_interval_sec
      .is_some_and(|seconds| !(30..=86_400).contains(&seconds))
  {
    return Err(error("VPN_POOL_ROTATION_INTERVAL_INVALID"));
  }
  normalized_country(request.country.as_deref())?;
  let known: std::collections::HashSet<_> = VPN_STORAGE
    .lock()
    .map_err(|_| error("INTERNAL_ERROR"))?
    .list_configs()
    .map_err(|_| error("INTERNAL_ERROR"))?
    .into_iter()
    .map(|config| config.id)
    .collect();
  if request.config_ids.iter().any(|id| !known.contains(id)) {
    return Err(error("VPN_CONFIG_NOT_FOUND"));
  }
  Ok(())
}

pub async fn create_pool(request: CreateVpnPoolRequest) -> Result<VpnPool, String> {
  validate_pool_request(&request)?;
  let now = now_secs() as i64;
  let pool = VpnPool {
    id: Uuid::new_v4().to_string(),
    name: request.name.trim().to_string(),
    provider_filter: request.provider_filter,
    country: normalized_country(request.country.as_deref())?,
    config_ids: request.config_ids,
    rotation_enabled: request.rotation_enabled,
    rotation_interval_sec: request.rotation_interval_sec,
    rotation_mode: request.rotation_mode,
    strategy: request.strategy,
    enabled: request.enabled,
    created_at: now,
    updated_at: now,
  };
  let mut state = STATE.lock().await;
  state.pools.push(pool.clone());
  state.save()?;
  Ok(pool)
}

pub async fn update_pool(id: &str, request: CreateVpnPoolRequest) -> Result<VpnPool, String> {
  validate_pool_request(&request)?;
  let mut state = STATE.lock().await;
  if state.leases.values().any(|lease| {
    lease.pool_id.as_deref() == Some(id)
      && matches!(
        lease.status,
        LeaseStatus::Provisioning | LeaseStatus::Active
      )
  }) || state
    .reservations
    .values()
    .any(|reservation| reservation.pool_id.as_deref() == Some(id))
  {
    return Err(error("VPN_POOL_HAS_ACTIVE_LEASE"));
  }
  let pool = state
    .pools
    .iter_mut()
    .find(|pool| pool.id == id)
    .ok_or_else(|| error("VPN_POOL_NOT_FOUND"))?;
  pool.name = request.name.trim().to_string();
  pool.provider_filter = request.provider_filter;
  pool.country = normalized_country(request.country.as_deref())?;
  pool.config_ids = request.config_ids;
  pool.rotation_enabled = request.rotation_enabled;
  pool.rotation_interval_sec = request.rotation_interval_sec;
  pool.rotation_mode = request.rotation_mode;
  pool.strategy = request.strategy;
  pool.enabled = request.enabled;
  pool.updated_at = now_secs() as i64;
  let result = pool.clone();
  state.save()?;
  Ok(result)
}

pub async fn delete_pool(id: &str) -> Result<(), String> {
  let mut state = STATE.lock().await;
  if state.leases.values().any(|lease| {
    lease.pool_id.as_deref() == Some(id)
      && matches!(
        lease.status,
        LeaseStatus::Provisioning | LeaseStatus::Active
      )
  }) || state
    .reservations
    .values()
    .any(|reservation| reservation.pool_id.as_deref() == Some(id))
  {
    return Err(error("VPN_POOL_HAS_ACTIVE_LEASE"));
  }
  if state
    .runtimes
    .get(id)
    .is_some_and(|runtime| runtime.status != VpnPoolStatus::Stopped)
  {
    return Err(error("VPN_POOL_RUNNING"));
  }
  let before = state.pools.len();
  state.pools.retain(|pool| pool.id != id);
  if before == state.pools.len() {
    return Err(error("VPN_POOL_NOT_FOUND"));
  }
  state.runtimes.remove(id);
  state.runtime_providers.remove(id);
  state.save()
}

async fn verify_config(
  local_port: u16,
  _generation: u64,
) -> Result<(String, Option<String>, u64), String> {
  let _permit = VERIFY_LIMIT
    .acquire()
    .await
    .map_err(|_| error("VPN_VERIFY_PROXY_FAILED"))?;
  let started = std::time::Instant::now();
  let mut last_error = error("VPN_VERIFY_PROXY_FAILED");
  let mut verified = None;
  for _ in 0..2 {
    let proxy_url = format!("socks5h://127.0.0.1:{local_port}");
    match tokio::time::timeout(
      std::time::Duration::from_secs(18),
      crate::ip_utils::fetch_public_ip(Some(&proxy_url)),
    )
    .await
    {
      Ok(Ok(ip)) => {
        let (_, _, country) = crate::proxy_manager::ProxyManager::get_ip_geolocation(&ip)
          .await
          .unwrap_or_default();
        verified = Some((ip, country));
        break;
      }
      Ok(Err(fetch_error)) => {
        last_error = error(classify_verify_error(&fetch_error.to_string()));
      }
      Err(_) => last_error = error("VPN_VERIFY_TIMEOUT"),
    }
  }
  let (ip, country) = verified.ok_or(last_error)?;
  Ok((ip, country, started.elapsed().as_millis() as u64))
}

async fn provision(
  config_id: &str,
  generation: u64,
) -> Result<
  (
    crate::vpn_worker_storage::VpnWorkerConfig,
    String,
    Option<String>,
    u64,
  ),
  String,
> {
  let worker = vpn_worker_runner::start_vpn_worker(config_id)
    .await
    .map_err(|_| error("VPN_WORKER_START_FAILED"))?;
  let Some(local_port) = worker.local_port else {
    let _ = vpn_worker_runner::stop_vpn_worker(&worker.id).await;
    return Err(error("VPN_WORKER_START_FAILED"));
  };
  match verify_config(local_port, generation).await {
    Ok((ip, country, latency)) => Ok((worker, ip, country, latency)),
    Err(error) => {
      let _ = vpn_worker_runner::stop_vpn_worker(&worker.id).await;
      Err(error)
    }
  }
}

pub async fn acquire_lease(request: AcquireVpnLeaseRequest) -> Result<VpnLease, String> {
  normalized_country(request.country.as_deref())?;
  if request
    .protocol
    .is_some_and(|protocol| protocol != ProxyProtocol::Socks5)
  {
    return Err(error("VPN_LEASE_PROTOCOL_INVALID"));
  }
  let max_wait = request
    .max_wait_seconds
    .unwrap_or(60)
    .clamp(1, MAX_WAIT_SECONDS);
  let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(max_wait);
  let mut excluded_config_ids = HashSet::new();
  let mut last_provision_error = None;
  let (config_id, provider, country, worker, exit_ip, exit_country) = loop {
    // Register before checking capacity so a release between the check and
    // await cannot be lost and leave this request sleeping until timeout.
    let capacity_available = CAPACITY_NOTIFY.notified();
    tokio::pin!(capacity_available);
    capacity_available.as_mut().enable();
    let reservation_id = Uuid::new_v4().to_string();
    let selection = {
      let mut state = STATE.lock().await;
      match state.select_candidate(&request, &excluded_config_ids) {
        Ok(value) => {
          state.reservations.insert(
            reservation_id.clone(),
            Reservation {
              config_id: value.0.clone(),
              provider: value.1,
              pool_id: request.pool_id.clone(),
            },
          );
          Some(Ok(value))
        }
        Err(error_value)
          if excluded_config_ids.is_empty()
            && request.wait_when_full
            && (error_value.contains("VPN_LEASE_CAPACITY")
              || error_value.contains("VPN_LEASE_NO_CONFIG")) =>
        {
          None
        }
        Err(error_value) => Some(Err(error_value)),
      }
    };
    let (config_id, provider, country) = match selection {
      Some(Err(selection_error))
        if selection_error.contains("VPN_LEASE_NO_CONFIG") && last_provision_error.is_some() =>
      {
        return Err(last_provision_error.unwrap_or(selection_error));
      }
      Some(result) => result?,
      None => {
        if tokio::time::timeout_at(deadline, capacity_available)
          .await
          .is_err()
        {
          return Err(error("VPN_LEASE_WAIT_TIMEOUT"));
        }
        continue;
      }
    };
    let generation = GENERATION.fetch_add(1, Ordering::SeqCst);
    let provisioned = provision(&config_id, generation).await;
    STATE.lock().await.reservations.remove(&reservation_id);
    CAPACITY_NOTIFY.notify_waiters();
    match provisioned {
      Ok((worker, exit_ip, exit_country, _)) => {
        break (config_id, provider, country, worker, exit_ip, exit_country);
      }
      Err(provision_error) => {
        excluded_config_ids.insert(config_id);
        last_provision_error = Some(provision_error);
      }
    }
  };
  let mut state = STATE.lock().await;
  let now = now_secs() as i64;
  let expires_at = request
    .ttl_seconds
    .filter(|seconds| *seconds > 0)
    .map(|seconds| now + seconds.min(86_400) as i64);
  let lease = VpnLease {
    id: Uuid::new_v4().to_string(),
    pool_id: request.pool_id,
    config_id,
    provider,
    country: exit_country.or(country),
    profile_id: request.profile_id,
    local_host: "127.0.0.1".to_string(),
    local_port: worker
      .local_port
      .ok_or_else(|| error("VPN_WORKER_START_FAILED"))?,
    protocol: ProxyProtocol::Socks5,
    exit_ip: Some(exit_ip),
    created_at: now,
    expires_at,
    status: LeaseStatus::Active,
  };
  state.leases.insert(lease.id.clone(), lease.clone());
  drop(state);
  if let Some(pool_id) = lease.pool_id.as_deref() {
    let _ = refresh_runtime_from_leases(pool_id).await;
  }
  monitor_lease_worker(lease.id.clone(), lease.config_id.clone());
  if let Some(expires_at) = lease.expires_at {
    let id = lease.id.clone();
    tokio::spawn(async move {
      let delay = (expires_at - now_secs() as i64).max(0) as u64;
      tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
      let _ = release_lease(&id).await;
    });
  }
  Ok(lease)
}

pub async fn refresh_runtime_from_leases(pool_id: &str) -> Result<VpnPoolRuntime, String> {
  let mut state = STATE.lock().await;
  if state.runtime_providers.contains_key(pool_id) {
    return state
      .runtimes
      .get(pool_id)
      .cloned()
      .ok_or_else(|| error("VPN_POOL_NOT_FOUND"));
  }
  let lease = state
    .leases
    .values()
    .find(|lease| lease.pool_id.as_deref() == Some(pool_id) && lease.status == LeaseStatus::Active)
    .cloned()
    .ok_or_else(|| error("VPN_LEASE_NOT_FOUND"))?;
  let pool = state
    .pools
    .iter()
    .find(|pool| pool.id == pool_id)
    .cloned()
    .ok_or_else(|| error("VPN_POOL_NOT_FOUND"))?;
  let generation = GENERATION.fetch_add(1, Ordering::SeqCst);
  let runtime = VpnPoolRuntime {
    pool_id: pool_id.to_string(),
    active_config_id: Some(lease.config_id),
    status: VpnPoolStatus::Connected,
    exit_ip: lease.exit_ip,
    exit_country: lease.country,
    next_rotation_at: pool
      .rotation_enabled
      .then(|| now_secs() as i64 + pool.rotation_interval_sec.unwrap_or(600) as i64),
    health: VpnHealth {
      latency_ms: None,
      verified_at: Some(now_secs() as i64),
      consecutive_failures: 0,
    },
    last_error_code: None,
    generation,
  };
  state.runtimes.insert(pool_id.to_string(), runtime.clone());
  state.runtime_providers.remove(pool_id);
  drop(state);
  if pool.rotation_enabled {
    schedule_rotation(
      pool_id.to_string(),
      generation,
      pool.rotation_interval_sec.unwrap_or(600),
    );
  }
  Ok(runtime)
}

pub async fn release_lease(id: &str) -> Result<bool, String> {
  let config_id = {
    let mut state = STATE.lock().await;
    let Some(lease) = state.leases.get_mut(id) else {
      return Ok(false);
    };
    if lease.status == LeaseStatus::Releasing {
      return Ok(true);
    }
    lease.status = LeaseStatus::Releasing;
    lease.config_id.clone()
  };
  if vpn_worker_runner::stop_vpn_worker_by_vpn_id(&config_id)
    .await
    .is_err()
  {
    let worker_is_alive = crate::vpn_worker_storage::find_vpn_worker_by_vpn_id(&config_id)
      .and_then(|worker| worker.pid)
      .is_some_and(crate::proxy_storage::is_process_running);
    if worker_is_alive {
      if let Some(lease) = STATE.lock().await.leases.get_mut(id) {
        lease.status = LeaseStatus::Active;
      }
      return Err(error("VPN_WORKER_STOP_FAILED"));
    }
  }
  let mut state = STATE.lock().await;
  state.leases.remove(id);
  drop(state);
  CAPACITY_NOTIFY.notify_waiters();
  Ok(true)
}

fn monitor_lease_worker(lease_id: String, config_id: String) {
  tokio::spawn(async move {
    loop {
      tokio::time::sleep(std::time::Duration::from_secs(2)).await;
      let still_active = STATE
        .lock()
        .await
        .leases
        .get(&lease_id)
        .is_some_and(|lease| {
          matches!(
            lease.status,
            LeaseStatus::Provisioning | LeaseStatus::Active
          )
        });
      if !still_active {
        break;
      }
      let worker_is_alive = crate::vpn_worker_storage::find_vpn_worker_by_vpn_id(&config_id)
        .and_then(|worker| worker.pid)
        .is_some_and(crate::proxy_storage::is_process_running);
      if !worker_is_alive {
        STATE.lock().await.leases.remove(&lease_id);
        CAPACITY_NOTIFY.notify_waiters();
        let _ = crate::events::emit("vpn-leases-updated", list_leases().await);
        break;
      }
    }
  });
}

pub async fn config_has_active_lease(config_id: &str) -> bool {
  let state = STATE.lock().await;
  state.leases.values().any(|lease| {
    lease.config_id == config_id
      && matches!(
        lease.status,
        LeaseStatus::Provisioning | LeaseStatus::Active | LeaseStatus::Releasing
      )
  }) || state
    .reservations
    .values()
    .any(|reservation| reservation.config_id == config_id)
}
pub async fn account_has_active_lease(account_id: &str) -> bool {
  let metadata = PROVIDER_STORE
    .lock()
    .ok()
    .and_then(|store| store.metadata().ok())
    .unwrap_or_default();
  let ids: std::collections::HashSet<_> = metadata
    .into_iter()
    .filter(|item| item.account_id == account_id)
    .map(|item| item.config_id)
    .collect();
  let state = STATE.lock().await;
  state.leases.values().any(|lease| {
    ids.contains(&lease.config_id)
      && matches!(
        lease.status,
        LeaseStatus::Provisioning | LeaseStatus::Active | LeaseStatus::Releasing
      )
  }) || state
    .reservations
    .values()
    .any(|reservation| ids.contains(&reservation.config_id))
}

pub async fn start_pool(id: &str) -> Result<VpnPoolRuntime, String> {
  rotate_pool_internal(id, false).await
}
pub async fn rotate_pool(id: &str) -> Result<VpnPoolRuntime, String> {
  rotate_pool_internal(id, true).await
}

async fn rotate_pool_internal(id: &str, rotating: bool) -> Result<VpnPoolRuntime, String> {
  let (pool, old_config, generation) = {
    let mut state = STATE.lock().await;
    if state.runtimes.get(id).is_some_and(|runtime| {
      matches!(
        runtime.status,
        VpnPoolStatus::Starting | VpnPoolStatus::Rotating
      )
    }) {
      return Err(error("VPN_POOL_RUNNING"));
    }
    if state.leases.values().any(|lease| {
      lease.pool_id.as_deref() == Some(id)
        && matches!(
          lease.status,
          LeaseStatus::Provisioning | LeaseStatus::Active
        )
    }) || state
      .reservations
      .values()
      .any(|reservation| reservation.pool_id.as_deref() == Some(id))
    {
      return Err(error("VPN_POOL_HAS_ACTIVE_LEASE"));
    }
    let pool = state
      .pools
      .iter()
      .find(|pool| pool.id == id)
      .cloned()
      .ok_or_else(|| error("VPN_POOL_NOT_FOUND"))?;
    if pool.rotation_mode == RotationMode::Hot {
      return Err(error("VPN_POOL_HOT_ROTATION_UNAVAILABLE"));
    }
    let old = state
      .runtimes
      .get(id)
      .and_then(|runtime| runtime.active_config_id.clone());
    let generation = GENERATION.fetch_add(1, Ordering::SeqCst);
    state.runtimes.insert(
      id.to_string(),
      VpnPoolRuntime {
        pool_id: id.to_string(),
        active_config_id: old.clone(),
        status: if rotating {
          VpnPoolStatus::Rotating
        } else {
          VpnPoolStatus::Starting
        },
        exit_ip: None,
        exit_country: None,
        next_rotation_at: None,
        health: VpnHealth::default(),
        last_error_code: None,
        generation,
      },
    );
    (pool, old, generation)
  };
  let metadata = PROVIDER_STORE
    .lock()
    .map_err(|_| error("INTERNAL_ERROR"))?
    .metadata()
    .map_err(|_| error("INTERNAL_ERROR"))?;
  let mut candidates: Vec<_> = metadata
    .iter()
    .filter(|item| Some(item.config_id.as_str()) != old_config.as_deref())
    .filter(|item| pool.config_ids.is_empty() || pool.config_ids.contains(&item.config_id))
    .filter(|item| pool.provider_filter.is_empty() || pool.provider_filter.contains(&item.provider))
    .filter(|item| {
      pool.country.as_ref().is_none_or(|country| {
        item
          .country
          .as_ref()
          .is_some_and(|value| value.eq_ignore_ascii_case(country))
      })
    })
    .map(|item| item.config_id.clone())
    .collect();
  {
    let mut state = STATE.lock().await;
    match pool.strategy {
      PoolSelectionStrategy::RoundRobin if !candidates.is_empty() => {
        let cursor = state.round_robin.entry(id.to_string()).or_default();
        let offset = *cursor % candidates.len();
        *cursor = cursor.wrapping_add(1);
        candidates.rotate_left(offset);
      }
      PoolSelectionStrategy::LeastRecentlyUsed => candidates
        .sort_by_key(|config_id| state.last_used.get(config_id).copied().unwrap_or_default()),
      PoolSelectionStrategy::RoundRobin => {}
    }
  }
  for config_id in candidates {
    let Some(config_metadata) = metadata.iter().find(|item| item.config_id == config_id) else {
      continue;
    };
    let account = PROVIDER_STORE
      .lock()
      .map_err(|_| error("INTERNAL_ERROR"))?
      .list_accounts()
      .map_err(|_| error("INTERNAL_ERROR"))?
      .into_iter()
      .find(|account| account.id == config_metadata.account_id);
    let Some(account) = account else {
      continue;
    };
    let reservation_id = Uuid::new_v4().to_string();
    {
      let mut state = STATE.lock().await;
      if state.config_busy(&config_id)
        || !has_global_capacity(state.active_count())
        || account.status != super::provider::ProviderAccountStatus::Ok
        || !has_provider_capacity(
          state.provider_count(config_metadata.provider),
          account.connection_cap,
        )
      {
        continue;
      }
      state.reservations.insert(
        reservation_id.clone(),
        Reservation {
          config_id: config_id.clone(),
          provider: config_metadata.provider,
          pool_id: Some(id.to_string()),
        },
      );
    }
    let provisioned = provision(&config_id, generation).await;
    STATE.lock().await.reservations.remove(&reservation_id);
    CAPACITY_NOTIFY.notify_waiters();
    if let Ok((worker, ip, country, latency)) = provisioned {
      let is_current = STATE
        .lock()
        .await
        .runtimes
        .get(id)
        .is_some_and(|runtime| runtime.generation == generation);
      if !is_current {
        let _ = vpn_worker_runner::stop_vpn_worker(&worker.id).await;
        return Err(error("VPN_VERIFY_STALE"));
      }
      if let Some(old) = &old_config {
        let _ = vpn_worker_runner::stop_vpn_worker_by_vpn_id(old).await;
      }
      let runtime = VpnPoolRuntime {
        pool_id: id.to_string(),
        active_config_id: Some(config_id.clone()),
        status: VpnPoolStatus::Connected,
        exit_ip: Some(ip),
        exit_country: country,
        next_rotation_at: pool
          .rotation_enabled
          .then(|| now_secs() as i64 + pool.rotation_interval_sec.unwrap_or(600) as i64),
        health: VpnHealth {
          latency_ms: Some(latency),
          verified_at: Some(now_secs() as i64),
          consecutive_failures: 0,
        },
        last_error_code: None,
        generation,
      };
      let mut state = STATE.lock().await;
      state.use_sequence = state.use_sequence.wrapping_add(1);
      let sequence = state.use_sequence;
      state.last_used.insert(config_id.clone(), sequence);
      state.runtimes.insert(id.to_string(), runtime.clone());
      state
        .runtime_providers
        .insert(id.to_string(), config_metadata.provider);
      drop(state);
      if pool.rotation_enabled {
        schedule_rotation(
          id.to_string(),
          generation,
          pool.rotation_interval_sec.unwrap_or(600),
        );
      }
      return Ok(runtime);
    }
  }
  let mut state = STATE.lock().await;
  let runtime = state
    .runtimes
    .entry(id.to_string())
    .or_insert(VpnPoolRuntime {
      pool_id: id.to_string(),
      active_config_id: old_config.clone(),
      status: VpnPoolStatus::Error,
      exit_ip: None,
      exit_country: None,
      next_rotation_at: None,
      health: VpnHealth::default(),
      last_error_code: Some("VPN_POOL_ROTATION_FAILED".to_string()),
      generation,
    });
  runtime.status = if old_config.is_some() {
    VpnPoolStatus::Degraded
  } else {
    VpnPoolStatus::Error
  };
  runtime.last_error_code = Some("VPN_POOL_ROTATION_FAILED".to_string());
  Err(error("VPN_POOL_ROTATION_FAILED"))
}

fn schedule_rotation(pool_id: String, generation: u64, interval_seconds: u64) {
  tokio::spawn(async move {
    tokio::time::sleep(std::time::Duration::from_secs(interval_seconds)).await;
    let should_rotate = STATE
      .lock()
      .await
      .runtimes
      .get(&pool_id)
      .is_some_and(|runtime| {
        runtime.generation == generation && runtime.status == VpnPoolStatus::Connected
      });
    if should_rotate {
      if let Some(app_handle) = APP_HANDLE.get().cloned() {
        let _ = crate::browser_runner::safe_rotate_vpn_pool(app_handle, &pool_id).await;
      } else {
        let _ = rotate_pool(&pool_id).await;
      }
    }
  });
}

pub async fn stop_pool(id: &str) -> Result<VpnPoolRuntime, String> {
  let config_id = {
    let state = STATE.lock().await;
    if state.runtimes.get(id).is_some_and(|runtime| {
      matches!(
        runtime.status,
        VpnPoolStatus::Starting | VpnPoolStatus::Rotating
      )
    }) {
      return Err(error("VPN_POOL_RUNNING"));
    }
    if state.leases.values().any(|lease| {
      lease.pool_id.as_deref() == Some(id)
        && matches!(
          lease.status,
          LeaseStatus::Provisioning | LeaseStatus::Active
        )
    }) || state
      .reservations
      .values()
      .any(|reservation| reservation.pool_id.as_deref() == Some(id))
    {
      return Err(error("VPN_POOL_HAS_ACTIVE_LEASE"));
    }
    state
      .runtimes
      .get(id)
      .and_then(|runtime| runtime.active_config_id.clone())
  };
  if let Some(config_id) = config_id {
    if vpn_worker_runner::stop_vpn_worker_by_vpn_id(&config_id)
      .await
      .is_err()
    {
      let worker_is_alive = crate::vpn_worker_storage::find_vpn_worker_by_vpn_id(&config_id)
        .and_then(|worker| worker.pid)
        .is_some_and(crate::proxy_storage::is_process_running);
      if worker_is_alive {
        return Err(error("VPN_WORKER_STOP_FAILED"));
      }
    }
  }
  let mut state = STATE.lock().await;
  let runtime = state
    .runtimes
    .entry(id.to_string())
    .or_insert(VpnPoolRuntime {
      pool_id: id.to_string(),
      active_config_id: None,
      status: VpnPoolStatus::Stopped,
      exit_ip: None,
      exit_country: None,
      next_rotation_at: None,
      health: VpnHealth::default(),
      last_error_code: None,
      generation: 0,
    });
  runtime.active_config_id = None;
  runtime.status = VpnPoolStatus::Stopped;
  runtime.exit_ip = None;
  runtime.exit_country = None;
  runtime.next_rotation_at = None;
  let result = runtime.clone();
  state.runtime_providers.remove(id);
  Ok(result)
}

pub async fn remove_config_references(config_id: &str) -> Result<(), String> {
  let mut state = STATE.lock().await;
  if state.config_busy(config_id) {
    return Err(error("VPN_CONFIG_HAS_ACTIVE_LEASE"));
  }
  let mut changed = false;
  for pool in &mut state.pools {
    let before = pool.config_ids.len();
    pool.config_ids.retain(|id| id != config_id);
    if before != pool.config_ids.len() {
      pool.updated_at = now_secs() as i64;
      changed = true;
    }
  }
  if changed {
    state.save()?;
  }
  Ok(())
}

pub async fn shutdown() {
  let leases: Vec<_> = STATE.lock().await.leases.keys().cloned().collect();
  for id in leases {
    let _ = release_lease(&id).await;
  }
  let pools: Vec<_> = STATE.lock().await.runtimes.keys().cloned().collect();
  for id in pools {
    let _ = stop_pool(&id).await;
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  fn state() -> PoolState {
    PoolState::with_path(tempfile::tempdir().unwrap().path().join("pools.json"))
  }
  #[test]
  fn ttl_zero_serializes_as_no_expiry() {
    let request: AcquireVpnLeaseRequest = serde_json::from_value(serde_json::json!({"pool_id":null,"country":null,"providers":[],"profile_id":null,"ttl_seconds":0,"protocol":"socks5","wait_when_full":false,"max_wait_seconds":1})).unwrap();
    assert_eq!(request.ttl_seconds.filter(|value| *value > 0), None);
  }
  #[test]
  fn capacity_counts_pending_reservations() {
    let mut state = state();
    state.reservations.insert(
      "r".into(),
      Reservation {
        config_id: "c".into(),
        provider: VpnProviderKind::Nordvpn,
        pool_id: Some("p".into()),
      },
    );
    assert_eq!(state.active_count(), 1);
  }
  #[test]
  fn capacity_counts_dedicated_pool_workers() {
    let mut state = state();
    state
      .runtime_providers
      .insert("pool".into(), VpnProviderKind::Piavpn);
    assert_eq!(state.active_count(), 1);
    assert_eq!(state.provider_count(VpnProviderKind::Piavpn), 1);
    assert_eq!(state.provider_count(VpnProviderKind::Nordvpn), 0);
  }
  #[test]
  fn config_busy_includes_reservation() {
    let mut state = state();
    state.reservations.insert(
      "r".into(),
      Reservation {
        config_id: "c".into(),
        provider: VpnProviderKind::Nordvpn,
        pool_id: Some("p".into()),
      },
    );
    assert!(state.config_busy("c"));
  }
  #[test]
  fn old_storage_defaults_to_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pools.json");
    fs::write(&path, "{}").unwrap();
    let parsed: PoolStorageData = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
    assert!(parsed.pools.is_empty());
  }

  #[test]
  fn round_robin_advances_and_wraps() {
    let ids = vec!["a".to_string(), "b".to_string()];
    let mut cursor = 0;
    let usage = HashMap::new();
    assert_eq!(
      strategy_index(PoolSelectionStrategy::RoundRobin, &ids, &mut cursor, &usage),
      0
    );
    assert_eq!(
      strategy_index(PoolSelectionStrategy::RoundRobin, &ids, &mut cursor, &usage),
      1
    );
    assert_eq!(
      strategy_index(PoolSelectionStrategy::RoundRobin, &ids, &mut cursor, &usage),
      0
    );
  }

  #[test]
  fn least_recently_used_prefers_never_used_then_oldest() {
    let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut cursor = 0;
    let mut usage = HashMap::from([("a".to_string(), 4), ("b".to_string(), 2)]);
    assert_eq!(
      strategy_index(
        PoolSelectionStrategy::LeastRecentlyUsed,
        &ids,
        &mut cursor,
        &usage,
      ),
      2
    );
    usage.insert("c".to_string(), 3);
    assert_eq!(
      strategy_index(
        PoolSelectionStrategy::LeastRecentlyUsed,
        &ids,
        &mut cursor,
        &usage,
      ),
      1
    );
  }

  #[test]
  fn capacity_boundaries_are_exclusive() {
    assert!(has_global_capacity(DEFAULT_GLOBAL_CAP - 1));
    assert!(!has_global_capacity(DEFAULT_GLOBAL_CAP));
    assert!(has_provider_capacity(1, 2));
    assert!(!has_provider_capacity(2, 2));
  }

  #[test]
  fn backend_errors_are_structured_codes() {
    let value: serde_json::Value = serde_json::from_str(&error("VPN_LEASE_CAPACITY")).unwrap();
    assert_eq!(value["code"], "VPN_LEASE_CAPACITY");
  }

  #[test]
  fn countries_are_validated_and_normalized() {
    assert_eq!(
      normalized_country(Some(" us ")).unwrap().as_deref(),
      Some("US")
    );
    assert_eq!(normalized_country(None).unwrap(), None);
    let value: serde_json::Value =
      serde_json::from_str(&normalized_country(Some("USA")).unwrap_err()).unwrap();
    assert_eq!(value["code"], "VPN_POOL_COUNTRY_INVALID");
  }

  #[test]
  fn verification_errors_preserve_failure_category() {
    assert_eq!(
      classify_verify_error("TLS certificate validation failed"),
      "VPN_VERIFY_TLS_FAILED"
    );
    assert_eq!(
      classify_verify_error("response is not an IP address"),
      "VPN_VERIFY_INVALID_RESPONSE"
    );
    assert_eq!(
      classify_verify_error("proxy connect refused"),
      "VPN_VERIFY_PROXY_FAILED"
    );
  }

  #[tokio::test]
  async fn releasing_an_unknown_lease_is_idempotent() {
    let id = format!("missing-{}", Uuid::new_v4());
    assert!(!release_lease(&id).await.unwrap());
    assert!(!release_lease(&id).await.unwrap());
  }
}
