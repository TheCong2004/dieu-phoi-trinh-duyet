use super::{VpnConfig, VpnError, VpnProviderKind, VpnType, VPN_STORAGE};
use base64::Engine;
use boringtun::x25519::{PublicKey, StaticSecret};
use chrono::Utc;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs, net::SocketAddr, path::PathBuf};
use uuid::Uuid;

const NORD_API: &str = "https://api.nordvpn.com/v1";
const NORD_DNS: &str = "103.86.96.100";
const PIA_TOKEN_URL: &str = "https://www.privateinternetaccess.com/api/client/v2/token";
const PIA_SERVERS_URL: &str = "https://serverlist.piaservers.net/vpninfo/servers/v6";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderAccountStatus {
  Ok,
  Expired,
  Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpnProviderAccount {
  pub id: String,
  pub label: String,
  pub provider: VpnProviderKind,
  pub auth_type: String,
  pub created_at: i64,
  pub status: ProviderAccountStatus,
  pub connection_cap: u32,
  pub has_credentials: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderCountry {
  pub id: Option<i64>,
  pub code: String,
  pub name: String,
  pub server_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderImportRequest {
  pub account_ids: Vec<String>,
  pub country: Option<String>,
  pub country_id: Option<i64>,
  pub count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderImportProgress {
  pub provider: VpnProviderKind,
  pub done: usize,
  pub total: usize,
  pub imported: usize,
  pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderImportResult {
  pub configs: Vec<VpnConfig>,
  pub failed: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedVpnMetadata {
  pub config_id: String,
  pub provider: VpnProviderKind,
  pub account_id: String,
  pub country: Option<String>,
  pub server_endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct ProviderData {
  #[serde(default)]
  accounts: Vec<StoredAccount>,
  #[serde(default)]
  imported_configs: Vec<ImportedVpnMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredAccount {
  account: VpnProviderAccount,
  encrypted_credentials: String,
  nonce: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Credentials {
  username: Option<String>,
  password: String,
}

pub struct ProviderStore {
  path: PathBuf,
}

impl Default for ProviderStore {
  fn default() -> Self {
    Self::new()
  }
}

impl ProviderStore {
  pub fn new() -> Self {
    Self {
      path: crate::app_dirs::vpn_dir().join("provider_accounts.json"),
    }
  }

  #[cfg(test)]
  fn with_dir(dir: &std::path::Path) -> Self {
    Self {
      path: dir.join("provider_accounts.json"),
    }
  }

  fn load(&self) -> Result<ProviderData, VpnError> {
    if !self.path.exists() {
      return Ok(ProviderData::default());
    }
    let content =
      fs::read_to_string(&self.path).map_err(|error| VpnError::Storage(error.to_string()))?;
    serde_json::from_str(&content).map_err(|error| VpnError::Storage(error.to_string()))
  }

  fn save(&self, data: &ProviderData) -> Result<(), VpnError> {
    if let Some(parent) = self.path.parent() {
      fs::create_dir_all(parent).map_err(|error| VpnError::Storage(error.to_string()))?;
    }
    let encoded =
      serde_json::to_string_pretty(data).map_err(|error| VpnError::Storage(error.to_string()))?;
    fs::write(&self.path, encoded).map_err(|error| VpnError::Storage(error.to_string()))?;
    crate::app_dirs::restrict_to_owner(&self.path);
    Ok(())
  }

  pub fn list_accounts(&self) -> Result<Vec<VpnProviderAccount>, VpnError> {
    Ok(
      self
        .load()?
        .accounts
        .into_iter()
        .map(|item| item.account)
        .collect(),
    )
  }

  fn credentials(&self, id: &str) -> Result<(VpnProviderAccount, Credentials), VpnError> {
    let item = self
      .load()?
      .accounts
      .into_iter()
      .find(|item| item.account.id == id)
      .ok_or_else(|| VpnError::NotFound(id.to_string()))?;
    let plaintext = VPN_STORAGE
      .lock()
      .map_err(|error| VpnError::Storage(error.to_string()))?
      .decrypt(&item.encrypted_credentials, &item.nonce)?;
    let credentials =
      serde_json::from_str(&plaintext).map_err(|error| VpnError::Encryption(error.to_string()))?;
    Ok((item.account, credentials))
  }

  pub fn add_account(
    &self,
    label: String,
    provider: VpnProviderKind,
    username: Option<String>,
    password: String,
  ) -> Result<VpnProviderAccount, VpnError> {
    let credentials = Credentials { username, password };
    let plaintext = serde_json::to_string(&credentials)
      .map_err(|error| VpnError::Encryption(error.to_string()))?;
    let (encrypted_credentials, nonce) = VPN_STORAGE
      .lock()
      .map_err(|error| VpnError::Storage(error.to_string()))?
      .encrypt(&plaintext)?;
    let account = VpnProviderAccount {
      id: Uuid::new_v4().to_string(),
      label,
      provider,
      auth_type: match provider {
        VpnProviderKind::Nordvpn => "token",
        VpnProviderKind::Piavpn => "username_password",
      }
      .to_string(),
      created_at: Utc::now().timestamp(),
      status: ProviderAccountStatus::Ok,
      connection_cap: match provider {
        VpnProviderKind::Nordvpn => 10,
        VpnProviderKind::Piavpn => 10,
      },
      has_credentials: true,
    };
    let mut data = self.load()?;
    data.accounts.push(StoredAccount {
      account: account.clone(),
      encrypted_credentials,
      nonce,
    });
    self.save(&data)?;
    Ok(account)
  }

  pub fn set_status(
    &self,
    id: &str,
    status: ProviderAccountStatus,
  ) -> Result<VpnProviderAccount, VpnError> {
    let mut data = self.load()?;
    let item = data
      .accounts
      .iter_mut()
      .find(|item| item.account.id == id)
      .ok_or_else(|| VpnError::NotFound(id.to_string()))?;
    item.account.status = status;
    let account = item.account.clone();
    self.save(&data)?;
    Ok(account)
  }

  pub fn delete_account(&self, id: &str) -> Result<(), VpnError> {
    let mut data = self.load()?;
    if data
      .imported_configs
      .iter()
      .any(|metadata| metadata.account_id == id)
    {
      return Err(VpnError::Storage("VPN_ACCOUNT_IN_USE".to_string()));
    }
    let before = data.accounts.len();
    data.accounts.retain(|item| item.account.id != id);
    if before == data.accounts.len() {
      return Err(VpnError::NotFound(id.to_string()));
    }
    self.save(&data)
  }

  pub fn metadata(&self) -> Result<Vec<ImportedVpnMetadata>, VpnError> {
    Ok(self.load()?.imported_configs)
  }

  pub fn metadata_for(&self, config_id: &str) -> Result<Option<ImportedVpnMetadata>, VpnError> {
    Ok(
      self
        .load()?
        .imported_configs
        .into_iter()
        .find(|metadata| metadata.config_id == config_id),
    )
  }

  pub fn add_metadata(&self, metadata: ImportedVpnMetadata) -> Result<(), VpnError> {
    let mut data = self.load()?;
    data
      .imported_configs
      .retain(|entry| entry.config_id != metadata.config_id);
    data.imported_configs.push(metadata);
    self.save(&data)
  }

  pub fn remove_config_reference(&self, config_id: &str) -> Result<(), VpnError> {
    let mut data = self.load()?;
    data
      .imported_configs
      .retain(|metadata| metadata.config_id != config_id);
    self.save(&data)
  }
}

#[derive(Debug, Deserialize)]
struct NordCredentialsResponse {
  nordlynx_private_key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NordCountryResponse {
  id: i64,
  code: String,
  name: String,
}

#[derive(Debug, Deserialize)]
struct NordServerResponse {
  name: Option<String>,
  hostname: Option<String>,
  station: Option<String>,
  #[serde(default)]
  technologies: Vec<NordTechnology>,
  #[serde(default)]
  locations: Vec<NordLocation>,
}

#[derive(Debug, Deserialize)]
struct NordTechnology {
  identifier: String,
  #[serde(default)]
  metadata: Vec<NordMetadata>,
}

#[derive(Debug, Deserialize)]
struct NordMetadata {
  name: String,
  value: String,
}

#[derive(Debug, Deserialize)]
struct NordLocation {
  country: NordLocationCountry,
}

#[derive(Debug, Deserialize)]
struct NordLocationCountry {
  code: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NordServer {
  name: String,
  endpoint: String,
  public_key: String,
  country: Option<String>,
}

fn parse_nord_servers(value: serde_json::Value) -> Result<Vec<NordServer>, VpnError> {
  let raw: Vec<NordServerResponse> =
    serde_json::from_value(value).map_err(|error| VpnError::Connection(error.to_string()))?;
  Ok(
    raw
      .into_iter()
      .filter_map(|server| {
        let endpoint = server.station?;
        let public_key = server
          .technologies
          .iter()
          .find(|technology| technology.identifier == "wireguard_udp")?
          .metadata
          .iter()
          .find(|metadata| metadata.name == "public_key")?
          .value
          .clone();
        Some(NordServer {
          name: server
            .name
            .or(server.hostname)
            .unwrap_or_else(|| endpoint.clone()),
          endpoint,
          public_key,
          country: server
            .locations
            .first()
            .map(|location| location.country.code.clone()),
        })
      })
      .collect(),
  )
}

#[derive(Debug, Deserialize)]
struct PiaRegionsResponse {
  #[serde(default)]
  regions: Vec<PiaRegion>,
}

#[derive(Debug, Deserialize)]
struct PiaRegion {
  id: String,
  name: String,
  country: String,
  servers: PiaServerKinds,
}

#[derive(Debug, Deserialize)]
struct PiaServerKinds {
  #[serde(default)]
  wg: Vec<PiaWireguardServer>,
}

#[derive(Debug, Deserialize, Clone)]
struct PiaWireguardServer {
  ip: String,
  cn: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PiaServer {
  ip: String,
  cn: String,
  region: String,
  country: String,
}

fn parse_pia_servers(text: &str) -> Result<Vec<PiaServer>, VpnError> {
  let first_line = text.lines().next().unwrap_or_default();
  let response: PiaRegionsResponse =
    serde_json::from_str(first_line).map_err(|error| VpnError::Connection(error.to_string()))?;
  Ok(
    response
      .regions
      .into_iter()
      .flat_map(|region| {
        region.servers.wg.into_iter().map(move |server| PiaServer {
          ip: server.ip,
          cn: server.cn,
          region: if region.name.is_empty() {
            region.id.clone()
          } else {
            region.name.clone()
          },
          country: region.country.clone(),
        })
      })
      .collect(),
  )
}

fn backend_error(code: &str) -> String {
  serde_json::json!({ "code": code }).to_string()
}

async fn nord_private_key(token: &str) -> Result<String, String> {
  let auth = base64::engine::general_purpose::STANDARD.encode(format!("token:{token}"));
  let response = reqwest::Client::new()
    .get(format!("{NORD_API}/users/services/credentials"))
    .header(reqwest::header::AUTHORIZATION, format!("Basic {auth}"))
    .timeout(std::time::Duration::from_secs(20))
    .send()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?;
  if matches!(response.status().as_u16(), 401 | 403) {
    return Err(backend_error("VPN_PROVIDER_ACCOUNT_EXPIRED"));
  }
  if !response.status().is_success() {
    return Err(backend_error("VPN_PROVIDER_UNAVAILABLE"));
  }
  response
    .json::<NordCredentialsResponse>()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?
    .nordlynx_private_key
    .ok_or_else(|| backend_error("VPN_PROVIDER_ACCOUNT_INVALID"))
}

async fn pia_token(username: &str, password: &str) -> Result<String, String> {
  let form = reqwest::multipart::Form::new()
    .text("username", username.to_string())
    .text("password", password.to_string());
  let response = reqwest::Client::new()
    .post(PIA_TOKEN_URL)
    .multipart(form)
    .timeout(std::time::Duration::from_secs(20))
    .send()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?;
  if matches!(response.status().as_u16(), 401..=403) {
    return Err(backend_error("VPN_PROVIDER_ACCOUNT_EXPIRED"));
  }
  let value: serde_json::Value = response
    .json()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
  value
    .get("token")
    .and_then(|token| token.as_str())
    .map(str::to_string)
    .ok_or_else(|| backend_error("VPN_PROVIDER_ACCOUNT_INVALID"))
}

pub async fn add_nord_account(label: String, token: String) -> Result<VpnProviderAccount, String> {
  let label = label.trim();
  let token = token.trim();
  if label.is_empty() || token.is_empty() {
    return Err(backend_error("VPN_PROVIDER_CREDENTIALS_REQUIRED"));
  }
  nord_private_key(token).await?;
  PROVIDER_STORE
    .lock()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .add_account(
      label.to_string(),
      VpnProviderKind::Nordvpn,
      None,
      token.to_string(),
    )
    .map_err(|_| backend_error("VPN_PROVIDER_ACCOUNT_SAVE_FAILED"))
}

pub async fn add_pia_account(
  label: String,
  username: String,
  password: String,
) -> Result<VpnProviderAccount, String> {
  let label = label.trim();
  let username = username.trim();
  if label.is_empty() || username.is_empty() || password.is_empty() {
    return Err(backend_error("VPN_PROVIDER_CREDENTIALS_REQUIRED"));
  }
  pia_token(username, &password).await?;
  PROVIDER_STORE
    .lock()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .add_account(
      label.to_string(),
      VpnProviderKind::Piavpn,
      Some(username.to_string()),
      password,
    )
    .map_err(|_| backend_error("VPN_PROVIDER_ACCOUNT_SAVE_FAILED"))
}

pub async fn validate_account(id: &str) -> Result<VpnProviderAccount, String> {
  let (account, credentials) = PROVIDER_STORE
    .lock()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .credentials(id)
    .map_err(|_| backend_error("VPN_PROVIDER_ACCOUNT_NOT_FOUND"))?;
  let result = match account.provider {
    VpnProviderKind::Nordvpn => nord_private_key(&credentials.password).await.map(|_| ()),
    VpnProviderKind::Piavpn => {
      let username = credentials
        .username
        .as_deref()
        .ok_or_else(|| backend_error("VPN_PROVIDER_ACCOUNT_INVALID"))?;
      pia_token(username, &credentials.password).await.map(|_| ())
    }
  };
  let status = status_for_validation_result(&result);
  let updated = PROVIDER_STORE
    .lock()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .set_status(id, status)
    .map_err(|_| backend_error("INTERNAL_ERROR"))?;
  result.map(|_| updated)
}

fn status_for_validation_result(result: &Result<(), String>) -> ProviderAccountStatus {
  match result {
    Ok(()) => ProviderAccountStatus::Ok,
    Err(error)
      if error.contains("VPN_PROVIDER_ACCOUNT_EXPIRED")
        || error.contains("VPN_PROVIDER_ACCOUNT_INVALID") =>
    {
      ProviderAccountStatus::Expired
    }
    Err(_) => ProviderAccountStatus::Error,
  }
}

pub async fn list_countries(provider: VpnProviderKind) -> Result<Vec<ProviderCountry>, String> {
  match provider {
    VpnProviderKind::Nordvpn => {
      let response = reqwest::Client::new()
        .get(format!("{NORD_API}/servers/countries"))
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?;
      let countries: Vec<NordCountryResponse> = response
        .json()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      Ok(
        countries
          .into_iter()
          .map(|country| ProviderCountry {
            id: Some(country.id),
            code: country.code,
            name: country.name,
            server_count: 0,
          })
          .collect(),
      )
    }
    VpnProviderKind::Piavpn => {
      let text = reqwest::Client::new()
        .get(PIA_SERVERS_URL)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?
        .text()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      let servers =
        parse_pia_servers(&text).map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      let mut countries = std::collections::BTreeMap::<String, (String, usize)>::new();
      for server in servers {
        let entry = countries
          .entry(server.country.clone())
          .or_insert((server.country, 0));
        entry.1 += 1;
      }
      Ok(
        countries
          .into_iter()
          .map(|(code, (name, server_count))| ProviderCountry {
            id: None,
            code,
            name,
            server_count,
          })
          .collect(),
      )
    }
  }
}

fn build_nord_config(private_key: &str, server: &NordServer) -> String {
  format!(
    "[Interface]\nPrivateKey = {private_key}\nAddress = 10.5.0.2/16\nDNS = {NORD_DNS}\n\n[Peer]\nPublicKey = {}\nAllowedIPs = 0.0.0.0/0\nEndpoint = {}:51820\nPersistentKeepalive = 25\n",
    server.public_key, server.endpoint
  )
}

#[derive(Debug, Deserialize)]
struct PiaAddKeyResponse {
  status: String,
  server_key: Option<String>,
  server_port: Option<u16>,
  peer_ip: Option<String>,
  #[serde(default)]
  dns_servers: Vec<String>,
}

async fn provision_pia(token: &str, server: &PiaServer) -> Result<String, String> {
  let private_bytes: [u8; 32] = rand::rng().random();
  let private_key = StaticSecret::from(private_bytes);
  let public_key = PublicKey::from(&private_key);
  let private_b64 = base64::engine::general_purpose::STANDARD.encode(private_key.to_bytes());
  let public_b64 = base64::engine::general_purpose::STANDARD.encode(public_key.as_bytes());
  let socket: SocketAddr = format!("{}:1337", server.ip)
    .parse()
    .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
  let client = reqwest::Client::builder()
    .resolve(&server.cn, socket)
    .timeout(std::time::Duration::from_secs(20))
    .build()
    .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?;
  let response = client
    .get(format!(
      "https://{}:1337/addKey?pt={}&pubkey={}",
      server.cn,
      urlencoding::encode(token),
      urlencoding::encode(&public_b64)
    ))
    .send()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_TLS_ERROR"))?;
  let registration: PiaAddKeyResponse = response
    .json()
    .await
    .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
  if registration.status != "OK" {
    return Err(backend_error("VPN_PROVIDER_PROVISION_FAILED"));
  }
  let peer_ip = registration
    .peer_ip
    .ok_or_else(|| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
  let server_key = registration
    .server_key
    .ok_or_else(|| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
  let dns = registration.dns_servers.join(", ");
  Ok(format!(
    "[Interface]\nPrivateKey = {private_b64}\nAddress = {peer_ip}\n{}\n[Peer]\nPublicKey = {server_key}\nAllowedIPs = 0.0.0.0/0\nEndpoint = {}:{}\nPersistentKeepalive = 25\n",
    if dns.is_empty() { String::new() } else { format!("DNS = {dns}\n") },
    server.ip,
    registration.server_port.unwrap_or(1337)
  ))
}

pub async fn import_configs<F>(
  provider: VpnProviderKind,
  request: ProviderImportRequest,
  mut progress: F,
) -> Result<ProviderImportResult, String>
where
  F: FnMut(ProviderImportProgress),
{
  if request.account_ids.is_empty() || request.count == 0 || request.count > 500 {
    return Err(backend_error("VPN_PROVIDER_IMPORT_INVALID"));
  }
  let mut accounts = Vec::new();
  let mut failed = 0usize;
  for id in &request.account_ids {
    let pair = match PROVIDER_STORE
      .lock()
      .map_err(|_| backend_error("INTERNAL_ERROR"))?
      .credentials(id)
    {
      Ok(pair) => pair,
      Err(_) => {
        failed += 1;
        continue;
      }
    };
    if !account_matches_provider(&pair.0, provider) {
      return Err(backend_error("VPN_PROVIDER_ACCOUNT_INVALID"));
    }
    if pair.0.status != ProviderAccountStatus::Ok {
      failed += 1;
      continue;
    }
    accounts.push(pair);
  }
  if accounts.is_empty() {
    return Err(backend_error("VPN_PROVIDER_ACCOUNT_INVALID"));
  }
  let existing: HashSet<String> = PROVIDER_STORE
    .lock()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .metadata()
    .map_err(|_| backend_error("INTERNAL_ERROR"))?
    .into_iter()
    .filter(|metadata| metadata.provider == provider)
    .map(|metadata| metadata.server_endpoint)
    .collect();
  let mut configs = Vec::new();
  match provider {
    VpnProviderKind::Nordvpn => {
      let mut valid_accounts = Vec::new();
      for (account, credentials) in &accounts {
        match nord_private_key(&credentials.password).await {
          Ok(key) => valid_accounts.push((account, key)),
          Err(_) => failed += 1,
        }
      }
      if valid_accounts.is_empty() {
        return Err(backend_error("VPN_PROVIDER_ACCOUNT_INVALID"));
      }
      let mut url = format!(
        "{NORD_API}/servers?filters[servers_technologies][identifier]=wireguard_udp&limit=6000"
      );
      if let Some(country_id) = request.country_id {
        url.push_str(&format!("&filters[country_id]={country_id}"));
      }
      let value = reqwest::Client::new()
        .get(url)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?
        .json()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      let servers =
        parse_nord_servers(value).map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      let candidates: Vec<_> = servers
        .into_iter()
        .filter(|server| !existing.contains(&server.endpoint))
        .take(request.count)
        .collect();
      for (index, server) in candidates.iter().enumerate() {
        let (account, private_key) = &valid_accounts[index % valid_accounts.len()];
        let content = build_nord_config(private_key, server);
        let name = format!("NordVPN {}", server.name);
        match VPN_STORAGE
          .lock()
          .map_err(|_| backend_error("INTERNAL_ERROR"))?
          .create_config_manual(&name, VpnType::WireGuard, &content)
        {
          Ok(config) => {
            PROVIDER_STORE
              .lock()
              .map_err(|_| backend_error("INTERNAL_ERROR"))?
              .add_metadata(ImportedVpnMetadata {
                config_id: config.id.clone(),
                provider,
                account_id: account.id.clone(),
                country: server.country.clone(),
                server_endpoint: server.endpoint.clone(),
              })
              .map_err(|_| backend_error("INTERNAL_ERROR"))?;
            let mut public_config = config;
            public_config.config_data.clear();
            configs.push(public_config);
          }
          Err(_) => failed += 1,
        }
        progress(ProviderImportProgress {
          provider,
          done: index + 1,
          total: candidates.len(),
          imported: configs.len(),
          failed,
        });
      }
    }
    VpnProviderKind::Piavpn => {
      let mut valid_accounts = Vec::new();
      for (account, credentials) in &accounts {
        let Some(username) = credentials.username.as_deref() else {
          failed += 1;
          continue;
        };
        match pia_token(username, &credentials.password).await {
          Ok(token) => valid_accounts.push((account, token)),
          Err(_) => failed += 1,
        }
      }
      if valid_accounts.is_empty() {
        return Err(backend_error("VPN_PROVIDER_ACCOUNT_INVALID"));
      }
      let text = reqwest::Client::new()
        .get(PIA_SERVERS_URL)
        .timeout(std::time::Duration::from_secs(20))
        .send()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_UNAVAILABLE"))?
        .text()
        .await
        .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?;
      let country = request.country.as_deref().map(str::to_uppercase);
      let candidates: Vec<_> = parse_pia_servers(&text)
        .map_err(|_| backend_error("VPN_PROVIDER_INVALID_RESPONSE"))?
        .into_iter()
        .filter(|server| {
          country
            .as_ref()
            .is_none_or(|value| server.country == *value)
        })
        .filter(|server| !existing.contains(&server.ip))
        .take(request.count)
        .collect();
      for (index, server) in candidates.iter().enumerate() {
        let (account, token) = &valid_accounts[index % valid_accounts.len()];
        match provision_pia(token, server).await {
          Ok(content) => {
            let name = format!("PIA {} ({})", server.region, server.cn);
            match VPN_STORAGE
              .lock()
              .map_err(|_| backend_error("INTERNAL_ERROR"))?
              .create_config_manual(&name, VpnType::WireGuard, &content)
            {
              Ok(config) => {
                PROVIDER_STORE
                  .lock()
                  .map_err(|_| backend_error("INTERNAL_ERROR"))?
                  .add_metadata(ImportedVpnMetadata {
                    config_id: config.id.clone(),
                    provider,
                    account_id: account.id.clone(),
                    country: Some(server.country.clone()),
                    server_endpoint: server.ip.clone(),
                  })
                  .map_err(|_| backend_error("INTERNAL_ERROR"))?;
                let mut public_config = config;
                public_config.config_data.clear();
                configs.push(public_config);
              }
              Err(_) => failed += 1,
            }
          }
          Err(_) => failed += 1,
        }
        progress(ProviderImportProgress {
          provider,
          done: index + 1,
          total: candidates.len(),
          imported: configs.len(),
          failed,
        });
      }
    }
  }
  if configs.is_empty() {
    return Err(backend_error("VPN_PROVIDER_IMPORT_EMPTY"));
  }
  Ok(ProviderImportResult { configs, failed })
}

fn account_matches_provider(account: &VpnProviderAccount, provider: VpnProviderKind) -> bool {
  account.provider == provider
}

use once_cell::sync::Lazy;
use std::sync::Mutex;

pub static PROVIDER_STORE: Lazy<Mutex<ProviderStore>> =
  Lazy::new(|| Mutex::new(ProviderStore::new()));

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn parses_nord_wireguard_servers() {
    let value = serde_json::json!([{
      "name": "United States #1",
      "station": "1.2.3.4",
      "technologies": [{"identifier":"wireguard_udp","metadata":[{"name":"public_key","value":"key"}]}],
      "locations": [{"country":{"code":"US"}}]
    }]);
    let servers = parse_nord_servers(value).unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].endpoint, "1.2.3.4");
    assert_eq!(servers[0].country.as_deref(), Some("US"));
  }

  #[test]
  fn parses_first_line_of_pia_server_response() {
    let text = "{\"regions\":[{\"id\":\"us_east\",\"name\":\"US East\",\"country\":\"US\",\"servers\":{\"wg\":[{\"ip\":\"1.2.3.4\",\"cn\":\"wg.example\"}]}}]}\nsignature";
    let servers = parse_pia_servers(text).unwrap();
    assert_eq!(servers.len(), 1);
    assert_eq!(servers[0].country, "US");
  }

  #[test]
  fn account_listing_never_contains_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let store = ProviderStore::with_dir(dir.path());
    let account = store
      .add_account(
        "Nord".to_string(),
        VpnProviderKind::Nordvpn,
        None,
        "secret-token".to_string(),
      )
      .unwrap();
    assert!(account.has_credentials);
    let serialized = serde_json::to_string(&store.list_accounts().unwrap()).unwrap();
    assert!(!serialized.contains("secret-token"));
  }

  #[test]
  fn provider_selection_is_strict() {
    let account = VpnProviderAccount {
      id: "account".to_string(),
      label: "PIA".to_string(),
      provider: VpnProviderKind::Piavpn,
      auth_type: "username_password".to_string(),
      created_at: 0,
      status: ProviderAccountStatus::Ok,
      connection_cap: 10,
      has_credentials: true,
    };
    assert!(account_matches_provider(&account, VpnProviderKind::Piavpn));
    assert!(!account_matches_provider(
      &account,
      VpnProviderKind::Nordvpn
    ));
  }

  #[test]
  fn validation_status_distinguishes_credentials_from_provider_failures() {
    assert_eq!(
      status_for_validation_result(&Ok(())),
      ProviderAccountStatus::Ok
    );
    assert_eq!(
      status_for_validation_result(&Err(backend_error("VPN_PROVIDER_ACCOUNT_EXPIRED"))),
      ProviderAccountStatus::Expired
    );
    assert_eq!(
      status_for_validation_result(&Err(backend_error("VPN_PROVIDER_UNAVAILABLE"))),
      ProviderAccountStatus::Error
    );
  }
}
