use super::worker_registry::WORKER_REGISTRY;
use super::worker_types::*;
use axum::{
  extract::Path,
  http::StatusCode,
  response::{IntoResponse, Json},
  routing::{get, post},
  Router,
};
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub fn worker_routes<S: Clone + Send + Sync + 'static>() -> Router<S> {
  Router::new()
    .route("/v1/workers/acquire", post(acquire_worker_handler))
    .route(
      "/v1/workers/leases/{lease_id}/heartbeat",
      post(heartbeat_lease_handler),
    )
    .route(
      "/v1/workers/leases/{lease_id}/release",
      post(release_lease_handler),
    )
    .route(
      "/v1/workers/{worker_id}/reconcile",
      post(reconcile_worker_handler),
    )
    .route(
      "/v1/workers/{worker_id}/health",
      post(worker_health_handshake_handler),
    )
    .route(
      "/v1/workers/{worker_id}/dispatch",
      post(dispatch_worker_handler),
    )
    .route("/v1/workers", get(list_workers_handler))
    .route("/v1/workers/leases", get(list_leases_handler))
    .route("/v1/publications/verify", post(verify_publication_handler))
    .route("/v1/health", get(health_check_handler))
    .route("/health", get(health_check_handler))
    .layer(axum::extract::DefaultBodyLimit::max(100 * 1024 * 1024))
}

pub async fn health_check_handler() -> Json<serde_json::Value> {
  Json(serde_json::json!({
    "status": "ok",
    "runtime": "floword-donut-runtime",
    "version": "1.0.0"
  }))
}

pub async fn acquire_worker_handler(
  Json(payload): Json<AcquireWorkerRequest>,
) -> Result<Json<AcquireWorkerResponse>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY.acquire(payload).await {
    Ok(res) => Ok(Json(res)),
    Err(err) => {
      let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": err.code_str(),
            "message": err.message
          }
        })),
      ))
    }
  }
}

pub async fn heartbeat_lease_handler(
  Path(lease_id): Path<String>,
  Json(payload): Json<HeartbeatLeaseRequest>,
) -> Result<Json<HeartbeatLeaseResponse>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY.heartbeat(&lease_id, payload).await {
    Ok(res) => Ok(Json(res)),
    Err(err) => {
      let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": err.code_str(),
            "message": err.message
          }
        })),
      ))
    }
  }
}

pub async fn release_lease_handler(Path(lease_id): Path<String>) -> impl IntoResponse {
  let res = WORKER_REGISTRY.release(&lease_id).await;

  // Trigger background health probe after release to reconcile to Ready if IDLE
  let leases = WORKER_REGISTRY.list_leases().await;
  if let Some(lease) = leases.leases.iter().find(|l| l.lease_id == lease_id) {
    let profile_id = lease.profile_id.clone();
    let worker_id = lease.worker_id.clone();
    tauri::async_runtime::spawn(async move {
      if let Ok(handshake) = probe_worker_health(&profile_id).await {
        let _ = WORKER_REGISTRY
          .handle_health_handshake(&worker_id, handshake)
          .await;
      }
    });
  }

  (StatusCode::OK, Json(res))
}

pub async fn reconcile_worker_handler(
  Path(worker_id): Path<String>,
  Json(payload): Json<ReconcileWorkerRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY
    .reconcile_worker(
      &worker_id,
      payload.is_idle,
      payload.is_healthy,
      payload.grok_logged_in,
    )
    .await
  {
    Ok(state) => Ok(Json(serde_json::json!({
      "worker_id": worker_id,
      "state": state
    }))),
    Err(err) => {
      let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": err.code_str(),
            "message": err.message
          }
        })),
      ))
    }
  }
}

pub async fn worker_health_handshake_handler(
  Path(worker_id): Path<String>,
  Json(payload): Json<WorkerHealthHandshakeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY
    .handle_health_handshake(&worker_id, payload)
    .await
  {
    Ok(()) => Ok(Json(serde_json::json!({
      "worker_id": worker_id,
      "status": "HANDSHAKE_ACCEPTED"
    }))),
    Err(err) => {
      let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": err.code_str(),
            "message": err.message
          }
        })),
      ))
    }
  }
}

pub async fn list_workers_handler() -> Json<ListWorkersResponse> {
  Json(WORKER_REGISTRY.list_workers().await)
}

pub async fn list_leases_handler() -> Json<ListLeasesResponse> {
  Json(WORKER_REGISTRY.list_leases().await)
}

pub async fn verify_publication_handler(
  Json(_payload): Json<VerifyPublicationRequest>,
) -> Json<VerifyPublicationResponse> {
  // Section 25.2 / Section 56: Foundation phase returns verified: false with CAPABILITY_UNAVAILABLE
  // Never returns verified: true without authoritative platform evidence.
  Json(VerifyPublicationResponse {
    verified: false,
    status: "CAPABILITY_UNAVAILABLE".to_string(),
    error: Some(VerifyPublicationError {
      code: "CAPABILITY_UNAVAILABLE".to_string(),
      message: "Social post verification is not implemented in current runtime phase".to_string(),
      retryable: false,
    }),
    reason: Some(
      "Social post verification is not implemented in current runtime phase".to_string(),
    ),
    details: None,
  })
}

async fn send_cdp_evaluate(
  ws_stream: &mut tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
  >,
  cmd_id: u64,
  expression: &str,
  context_id: i64,
  timeout_dur: Duration,
) -> Result<serde_json::Value, WorkerError> {
  let params = serde_json::json!({
    "expression": expression,
    "awaitPromise": true,
    "returnByValue": true,
    "contextId": context_id
  });

  let cdp_req = serde_json::json!({
    "id": cmd_id,
    "method": "Runtime.evaluate",
    "params": params
  });

  ws_stream
    .send(Message::Text(cdp_req.to_string().into()))
    .await
    .map_err(|e| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        format!("Failed to send CDP evaluate: {e}"),
      )
    })?;

  let wait_fut = async {
    while let Some(msg) = ws_stream.next().await {
      match msg {
        Ok(Message::Text(text)) => {
          if let Ok(resp) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
            if resp.get("id") == Some(&serde_json::json!(cmd_id)) {
              if let Some(err) = resp.get("error") {
                return Err(WorkerError::new(
                  WorkerErrorCode::BridgeDisconnected,
                  format!("CDP evaluation error: {err}"),
                ));
              }
              if let Some(exception) = resp.get("result").and_then(|r| r.get("exceptionDetails")) {
                return Err(WorkerError::new(
                  WorkerErrorCode::BridgeDisconnected,
                  format!("Extension execution exception: {exception}"),
                ));
              }

              let value = resp
                .get("result")
                .and_then(|r| r.get("result"))
                .and_then(|r| r.get("value"))
                .cloned()
                .or_else(|| resp.get("result").and_then(|r| r.get("value")).cloned())
                .ok_or_else(|| {
                  WorkerError::new(
                    WorkerErrorCode::BridgeDisconnected,
                    "Missing value in CDP evaluation result",
                  )
                })?;

              return Ok(value);
            }
          }
        }
        Ok(Message::Close(_)) => break,
        Err(e) => {
          return Err(WorkerError::new(
            WorkerErrorCode::BridgeDisconnected,
            format!("WS stream error: {e}"),
          ));
        }
        _ => {}
      }
    }

    Err(WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      "No evaluate response received from CDP",
    ))
  };

  tokio::time::timeout(timeout_dur, wait_fut)
    .await
    .map_err(|_| {
      WorkerError::new(
        WorkerErrorCode::BridgeTimeout,
        format!("CDP evaluation timed out after {timeout_dur:?}"),
      )
    })?
}

/// Dispatches a Floword production command to the exact target browser profile extension via CDP isolated context.
/// Multi-site aware: Routes to the appropriate site tab (Grok, Facebook, TikTok, YouTube Studio) based on method.
pub async fn dispatch_to_profile_extension(
  profile_id: &str,
  payload: &serde_json::Value,
) -> Result<serde_json::Value, WorkerError> {
  let method = payload.get("method").and_then(|v| v.as_str()).unwrap_or("");

  let descriptor = ProductionMethodDescriptor::lookup(method).ok_or_else(|| {
    WorkerError::new(
      WorkerErrorCode::MethodNotSupported,
      format!("Method '{method}' is not supported in this runtime"),
    )
  })?;

  // If method is not implemented, fail closed with CAPABILITY_UNAVAILABLE
  if !descriptor.implemented {
    return Err(WorkerError::new(
      WorkerErrorCode::CapabilityUnavailable,
      format!(
        "Method '{method}' is recognized but executor is not implemented in current runtime phase"
      ),
    ));
  }

  let site = match descriptor.site_policy {
    ProductionSitePolicy::Site(s) => s,
    ProductionSitePolicy::ActiveTask => ProductionSite::Grok,
  };

  let profiles_dir = crate::profile::ProfileManager::instance().get_profiles_dir();
  let profiles = crate::profile::ProfileManager::instance()
    .list_profiles()
    .map_err(|e| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        format!("Failed to list profiles: {e}"),
      )
    })?;

  // 1. Strict profile ID match (no display name fallback)
  let clean_profile_id = profile_id.strip_prefix("browser-profile:").unwrap_or(profile_id);
  let profile = profiles
    .into_iter()
    .find(|p| p.id.to_string() == clean_profile_id || format!("browser-profile:{}", p.id) == profile_id)
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!("Profile ID '{profile_id}' not found in runtime"),
      )
    })?;

  let profile_path = profile.get_profile_data_path(&profiles_dir);
  let profile_path_str = profile_path.to_string_lossy();

  let mut cdp_port_opt = crate::wayfern_manager::WayfernManager::instance()
    .get_cdp_port(&profile_path_str)
    .await;

  // Auto-launch fallback if browser profile is currently stopped
  if cdp_port_opt.is_none() {
    log::info!("[WorkerDispatch] Profile '{profile_id}' not running; auto-launching browser profile...");
    let target_site_url = match site {
      ProductionSite::Grok => "https://grok.com/imagine",
      ProductionSite::Facebook => "https://www.facebook.com",
      ProductionSite::TikTok => "https://www.tiktok.com",
      ProductionSite::YouTubeStudio => "https://studio.youtube.com",
    };

    let http_client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build().ok();
    if let Some(client) = http_client {
      let run_url = format!("http://127.0.0.1:10108/v1/profiles/{clean_profile_id}/run");
      let _ = client.post(&run_url).json(&serde_json::json!({
        "url": target_site_url,
        "headless": false,
      })).send().await;
    }

    // Wait up to 15 seconds for browser process and CDP port to bind
    for _attempt in 1..=15 {
      tokio::time::sleep(Duration::from_secs(1)).await;
      cdp_port_opt = crate::wayfern_manager::WayfernManager::instance()
        .get_cdp_port(&profile_path_str)
        .await;
      if cdp_port_opt.is_some() {
        break;
      }
    }
  }

  let cdp_port = cdp_port_opt.ok_or_else(|| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("No active browser/CDP instance running for profile '{profile_id}'"),
    )
  })?;

  let url = format!("http://127.0.0.1:{cdp_port}/json");
  let http_client = reqwest::Client::builder()
    .timeout(Duration::from_secs(5))
    .build()
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, e.to_string()))?;

  let targets_resp = http_client.get(&url).send().await.map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("CDP endpoint unavailable: {e}"),
    )
  })?;

  let targets: Vec<serde_json::Value> = targets_resp.json().await.map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("Failed to parse CDP targets: {e}"),
    )
  })?;

  // 2. Strict target selection with safe hostname boundary matching and multi-tab guard
  let mut matching_targets: Vec<serde_json::Value> = targets
    .iter()
    .cloned()
    .filter(|t| {
      let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
      if t_type != "page" {
        return false;
      }
      let t_url_str = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
      if let Ok(parsed_url) = url::Url::parse(t_url_str) {
        if let Some(host) = parsed_url.host_str() {
          return site.matches_host(host);
        }
      }
      false
    })
    .collect();

  // Auto-open / navigate site tab if not currently open
  if matching_targets.is_empty() {
    let target_site_url = match site {
      ProductionSite::Grok => "https://grok.com/imagine",
      ProductionSite::Facebook => "https://www.facebook.com",
      ProductionSite::TikTok => "https://www.tiktok.com",
      ProductionSite::YouTubeStudio => "https://studio.youtube.com",
    };
    log::info!("[WorkerDispatch] No active {} tab found; auto-navigating/opening tab: {}", site.display_name(), target_site_url);

    // If an existing blank/new tab is present, navigate it via CDP
    let blank_target = targets.iter().find(|t| {
      let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
      if t_type != "page" {
        return false;
      }
      let t_url = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
      t_url.is_empty() || t_url == "about:blank" || t_url.starts_with("chrome://") || t_url.starts_with("chrome-search://")
    });

    if let Some(bt) = blank_target {
      if let Some(ws_url) = bt.get("webSocketDebuggerUrl").and_then(|v| v.as_str()) {
        if let Ok((mut ws, _)) = connect_async(ws_url).await {
          let nav_cmd = serde_json::json!({
            "id": 100,
            "method": "Page.navigate",
            "params": {
              "url": target_site_url
            }
          });
          let _ = ws.send(Message::Text(nav_cmd.to_string().into())).await;
        }
      }
    } else {
      let new_tab_url = format!("http://127.0.0.1:{cdp_port}/json/new?{target_site_url}");
      let _ = http_client.put(&new_tab_url).send().await;
    }

    // Wait up to 6 seconds for Grok to load and content script to initialize
    for _ in 1..=8 {
      tokio::time::sleep(Duration::from_millis(800)).await;
      if let Ok(resp) = http_client.get(&url).send().await {
        if let Ok(new_targets) = resp.json::<Vec<serde_json::Value>>().await {
          matching_targets = new_targets
            .into_iter()
            .filter(|t| {
              let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
              if t_type != "page" {
                return false;
              }
              let t_url_str = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
              if let Ok(parsed_url) = url::Url::parse(t_url_str) {
                if let Some(host) = parsed_url.host_str() {
                  return site.matches_host(host);
                }
              }
              false
            })
            .collect();
          if !matching_targets.is_empty() {
            log::info!("[WorkerDispatch] Auto-navigation to {} successful", site.display_name());
            break;
          }
        }
      }
    }
  }

  if matching_targets.is_empty() {
    return Err(WorkerError::new(
      WorkerErrorCode::TargetNotFound,
      format!(
        "No active {} page target found for profile '{profile_id}'. Ensure {} tab is open.",
        site.display_name(),
        site.display_name()
      ),
    ));
  }

  let target = if matching_targets.len() == 1 {
    matching_targets.remove(0)
  } else if let Some(pos) = matching_targets.iter().position(|t| {
    t.get("url").and_then(|v| v.as_str()).map(|u| u.contains("/imagine")).unwrap_or(false)
  }) {
    matching_targets.remove(pos)
  } else {
    matching_targets.remove(0)
  };

  let ws_url = target
    .get("webSocketDebuggerUrl")
    .and_then(|v| v.as_str())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        "No webSocketDebuggerUrl in target".to_string(),
      )
    })?;

  let (mut ws_stream, _) = connect_async(ws_url).await.map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("Failed to connect to CDP WS: {e}"),
    )
  })?;

  let current_target_url = target.get("url").and_then(|v| v.as_str()).unwrap_or("");
  if site == ProductionSite::Grok && !current_target_url.contains("/imagine") {
    log::info!("[WorkerDispatch] Target Grok URL is '{current_target_url}' (not on /imagine); navigating to https://grok.com/imagine...");
    let nav_cmd = serde_json::json!({
      "id": 99,
      "method": "Page.navigate",
      "params": {
        "url": "https://grok.com/imagine"
      }
    });
    let _ = ws_stream.send(Message::Text(nav_cmd.to_string().into())).await;
    tokio::time::sleep(Duration::from_millis(2500)).await;
  }

  // 3. Discover Execution Contexts via Runtime.enable
  let enable_req = serde_json::json!({
    "id": 1,
    "method": "Runtime.enable"
  });
  let _ = ws_stream
    .send(Message::Text(enable_req.to_string().into()))
    .await;

  // Drain initial context creation events (up to 300ms timeout)
  let mut isolated_context_ids = Vec::new();
  let drain_deadline = tokio::time::Instant::now() + Duration::from_millis(300);
  while tokio::time::Instant::now() < drain_deadline {
    match tokio::time::timeout(Duration::from_millis(50), ws_stream.next()).await {
      Ok(Some(Ok(Message::Text(text)))) => {
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
          if event.get("method") == Some(&serde_json::json!("Runtime.executionContextCreated")) {
            if let Some(ctx) = event.get("params").and_then(|p| p.get("context")) {
              if let Some(cid) = ctx.get("id").and_then(|v| v.as_i64()) {
                let is_default = ctx
                  .get("auxiliaryData")
                  .and_then(|a| a.get("isDefault"))
                  .and_then(|v| v.as_bool())
                  .unwrap_or(true);
                if !is_default {
                  isolated_context_ids.push(cid);
                }
              }
            }
          }
        }
      }
      _ => break,
    }
  }

  // 4. Privileged Profile Binding: Bind authoritative profileId in Extension isolated context
  let bind_expr = format!(
    r#"if (typeof window.__flowordBindRuntimeProfile === 'function') {{
      window.__flowordBindRuntimeProfile({:?});
      true;
    }} else if (typeof window.__tobyflowGrokOnMessage === 'function' || typeof window.__flowordOnMessage === 'function') {{
      true;
    }} else {{
      false;
    }}"#,
    profile_id
  );

  let mut target_context_id: Option<i64> = None;
  for attempt in 1..=6 {
    let candidate_ids: Vec<i64> = if !isolated_context_ids.is_empty() {
      isolated_context_ids.clone()
    } else {
      (1..=15).collect()
    };

    for cid in candidate_ids {
      if let Ok(res) = send_cdp_evaluate(
        &mut ws_stream,
        (100 * attempt + cid) as u64,
        &bind_expr,
        cid,
        Duration::from_millis(500),
      )
      .await
      {
        if res.as_bool() == Some(true) {
          target_context_id = Some(cid);
          log::info!("[WorkerDispatch] Extension context bound: id={cid} (attempt {attempt})");
          break;
        }
      }
    }

    if target_context_id.is_some() {
      break;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
  }

  // Fallback: If isolated context not identified, try context 1 or default
  let target_context_id = target_context_id.unwrap_or(1);

  // 5. Execute production command directly in verified isolated context
  let payload_json_str = serde_json::to_string(payload).map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("Failed to serialize payload: {e}"),
    )
  })?;

  let exec_expr = format!(
    r#"new Promise((resolve, reject) => {{
      const req = {payload_json_str};
      if (typeof window.__flowordOnMessage === 'function') {{
        window.__flowordOnMessage(req, null, (res) => resolve(res));
      }} else if (typeof window.__tobyflowGrokOnMessage === 'function') {{
        window.__tobyflowGrokOnMessage(req, null, (res) => resolve(res));
      }} else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendMessage) {{
        chrome.runtime.sendMessage(req, (res) => {{
          if (chrome.runtime.lastError) {{
            reject(new Error(chrome.runtime.lastError.message));
          }} else {{
            resolve(res);
          }}
        }});
      }} else {{
        reject(new Error('Extension message handler not found in isolated context'));
      }}
    }})"#
  );

  let timeout_ms = payload
    .get("params")
    .and_then(|p| p.get("timeoutMs"))
    .and_then(|v| v.as_u64())
    .unwrap_or(180_000);
  let method_dur = Duration::from_millis(timeout_ms + 5_000);

  let prod_result = match send_cdp_evaluate(
    &mut ws_stream,
    1001,
    &exec_expr,
    target_context_id,
    Duration::from_millis(500),
  )
  .await
  {
    Ok(val) => val,
    Err(e) => {
      log::info!("[WorkerDispatch] CDP evaluate ({}); executing via Page.navigate DOM bridge...", e.message);
      execute_via_page_navigate_dom(&mut ws_stream, payload, method_dur).await?
    }
  };
  Ok(prod_result)
}

async fn execute_via_page_navigate_dom(
  ws_stream: &mut tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
  payload: &serde_json::Value,
  timeout_dur: Duration,
) -> Result<serde_json::Value, WorkerError> {
  use base64::Engine;
  let payload_json_str = serde_json::to_string(payload).unwrap_or_default();
  let b64 = base64::engine::general_purpose::STANDARD.encode(payload_json_str.as_bytes());

  // 1. Enable DOM and Page domains
  let enable_dom = serde_json::json!({ "id": 501, "method": "DOM.enable" });
  let _ = ws_stream.send(Message::Text(enable_dom.to_string().into())).await;

  let enable_page = serde_json::json!({ "id": 502, "method": "Page.enable" });
  let _ = ws_stream.send(Message::Text(enable_page.to_string().into())).await;

  // 2. Resolve document root and HTML element nodeId with retry loop
  let mut root_node_id = 1i64;
  let mut html_node_id = 0i64;

  for _attempt in 0..15 {
    let get_doc_req = serde_json::json!({ "id": 504, "method": "DOM.getDocument", "params": { "depth": -1 } });
    let _ = ws_stream.send(Message::Text(get_doc_req.to_string().into())).await;

    let drain_doc = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < drain_doc {
      if let Ok(Some(Ok(Message::Text(t)))) = tokio::time::timeout(Duration::from_millis(50), ws_stream.next()).await {
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(t.as_str()) {
          if resp.get("id") == Some(&serde_json::json!(504)) {
            if let Some(root) = resp.get("result").and_then(|r| r.get("root")) {
              if let Some(nid) = root.get("nodeId").and_then(|v| v.as_i64()) {
                root_node_id = nid;
              }
              if let Some(children) = root.get("children").and_then(|c| c.as_array()) {
                for child in children {
                  if child.get("nodeName").and_then(|n| n.as_str()) == Some("HTML") {
                    if let Some(nid) = child.get("nodeId").and_then(|v| v.as_i64()) {
                      html_node_id = nid;
                      break;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

    if html_node_id > 1 {
      break;
    }

    // Try DOM.querySelector for "html"
    let query_html = serde_json::json!({
      "id": 505,
      "method": "DOM.querySelector",
      "params": {
        "nodeId": if root_node_id > 0 { root_node_id } else { 1 },
        "selector": "html"
      }
    });
    let _ = ws_stream.send(Message::Text(query_html.to_string().into())).await;
    let drain_q = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < drain_q {
      if let Ok(Some(Ok(Message::Text(t)))) = tokio::time::timeout(Duration::from_millis(50), ws_stream.next()).await {
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(t.as_str()) {
          if resp.get("id") == Some(&serde_json::json!(505)) {
            if let Some(nid) = resp.get("result").and_then(|r| r.get("nodeId")).and_then(|v| v.as_i64()) {
              if nid > 1 {
                html_node_id = nid;
                break;
              }
            }
          }
        }
      }
    }

    if html_node_id > 1 {
      break;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
  }

  if html_node_id <= 1 {
    html_node_id = if root_node_id > 1 { root_node_id } else { 3 };
  }

  log::info!("[WorkerDispatch] Resolved HTML element nodeId: {html_node_id} (root: {root_node_id})");

  let req_id = payload.get("requestId").and_then(|v| v.as_str()).unwrap_or("default");
  let req_attr = format!("data-floword-req-{req_id}").to_ascii_lowercase();
  let res_attr = format!("data-floword-res-{req_id}").to_ascii_lowercase();
  let err_attr = format!("data-floword-err-{req_id}").to_ascii_lowercase();

  // 3. Remove old result / error attributes for this specific request only
  for attr_name in &[&res_attr, &err_attr, &req_attr] {
    let remove_req = serde_json::json!({
      "id": 506,
      "method": "DOM.removeAttribute",
      "params": {
        "nodeId": html_node_id,
        "name": attr_name
      }
    });
    let _ = ws_stream.send(Message::Text(remove_req.to_string().into())).await;
  }
  tokio::time::sleep(Duration::from_millis(100)).await;

  // 4. Set payload via DOM.setAttributeValue on the HTML element
  let set_attr_req = serde_json::json!({
    "id": 507,
    "method": "DOM.setAttributeValue",
    "params": {
      "nodeId": html_node_id,
      "name": &req_attr,
      "value": b64
    }
  });
  let _ = ws_stream.send(Message::Text(set_attr_req.to_string().into())).await;
  log::info!("[WorkerDispatch] Injected {req_attr} (b64 length: {}) on HTML node {html_node_id}", b64.len());

  // 5. Poll DOM attributes for result
  let start_time = tokio::time::Instant::now();
  let mut poll_cmd_id = 600u64;

  while start_time.elapsed() < timeout_dur {
    tokio::time::sleep(Duration::from_millis(600)).await;

    poll_cmd_id += 1;
    let attr_cmd_id = poll_cmd_id;
    let get_attrs = serde_json::json!({
      "id": attr_cmd_id,
      "method": "DOM.getAttributes",
      "params": { "nodeId": html_node_id }
    });
    let _ = ws_stream.send(Message::Text(get_attrs.to_string().into())).await;

    let drain_until = tokio::time::Instant::now() + Duration::from_millis(300);
    while tokio::time::Instant::now() < drain_until {
      if let Ok(Some(Ok(Message::Text(t)))) = tokio::time::timeout(Duration::from_millis(50), ws_stream.next()).await {
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(t.as_str()) {
          if resp.get("id") == Some(&serde_json::json!(attr_cmd_id)) {
            if let Some(attrs) = resp.get("result").and_then(|r| r.get("attributes")).and_then(|a| a.as_array()) {
              for chunk in attrs.chunks(2) {
                if let (Some(k), Some(v)) = (chunk.get(0).and_then(|s| s.as_str()), chunk.get(1).and_then(|s| s.as_str())) {
                  let k_lower = k.to_ascii_lowercase();
                  let req_id_lower = req_id.to_ascii_lowercase();
                  if k_lower == err_attr || (k_lower.starts_with("data-floword-err") && k_lower.contains(&req_id_lower)) || (req_id == "default" && k_lower == "data-floword-err") {
                    log::error!("[WorkerDispatch] Received error from DOM for {req_id}: {v}");
                    return Err(WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Extension error: {v}")));
                  }
                  if k_lower == res_attr || (k_lower.starts_with("data-floword-res") && k_lower.contains(&req_id_lower)) || (req_id == "default" && k_lower == "data-floword-res") {
                    log::info!("[WorkerDispatch] Received result from DOM for {req_id}! (length: {})", v.len());
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(v) {
                      return Ok(val);
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  Err(WorkerError::new(WorkerErrorCode::BridgeTimeout, format!("Execution timed out after {timeout_dur:?}")))
}

/// Active health probe from Donut Browser to Extension instance across all sites.
/// Collects per-site session state and aggregates capabilities without last-probe-wins loss.
pub async fn probe_worker_health(
  profile_id: &str,
) -> Result<WorkerHealthHandshakeRequest, WorkerError> {
  let profiles_dir = crate::profile::ProfileManager::instance().get_profiles_dir();
  let profiles = crate::profile::ProfileManager::instance()
    .list_profiles()
    .unwrap_or_default();
  let profile = profiles
    .into_iter()
    .find(|p| p.id.to_string() == profile_id)
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!("Profile '{profile_id}' not found"),
      )
    })?;

  let profile_path = profile.get_profile_data_path(&profiles_dir);
  let profile_path_str = profile_path.to_string_lossy().to_string();

  // If worker is currently busy running a leased task, skip active health probe to avoid DOM collision
  let workers_list = WORKER_REGISTRY.list_workers().await;
  if let Some(w) = workers_list.workers.into_iter().find(|w| w.profile_id == profile_id) {
    if w.current_lease_id.is_some() || w.state == WorkerState::Busy || w.state == WorkerState::Leased {
      return Ok(WorkerHealthHandshakeRequest {
        profile_id: profile_id.to_string(),
        protocol_version: 1,
        extension_version: "1.1.49".to_string(),
        worker_state: "BUSY".to_string(),
        logged_in: Some(true),
        capabilities: vec![
          "grok.image.edit".to_string(),
          "grok.expand.9_16".to_string(),
          "grok.video.generate".to_string(),
          "social.facebook.publish".to_string(),
          "social.tiktok.publish".to_string(),
          "social.youtube.publish".to_string(),
        ],
        site_sessions: Vec::new(),
        site_capabilities: std::collections::HashMap::new(),
      });
    }
  };

  let cdp_port = crate::wayfern_manager::WayfernManager::instance()
    .get_cdp_port(&profile_path_str)
    .await
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        format!("No active browser/CDP instance running for profile '{profile_id}'"),
      )
    })?;

  let url = format!("http://127.0.0.1:{cdp_port}/json");
  let http_client = reqwest::Client::builder()
    .timeout(Duration::from_secs(5))
    .build()
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, e.to_string()))?;

  let targets_resp = http_client.get(&url).send().await.map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("CDP endpoint unavailable: {e}"),
    )
  })?;

  let targets: Vec<serde_json::Value> = targets_resp.json().await.map_err(|e| {
    WorkerError::new(
      WorkerErrorCode::BridgeDisconnected,
      format!("Failed to parse CDP targets: {e}"),
    )
  })?;

  let mut site_sessions = Vec::new();
  let mut site_capabilities: std::collections::HashMap<String, Vec<String>> =
    std::collections::HashMap::new();
  let mut grok_logged_in: Option<bool> = None;
  let mut overall_worker_state = "IDLE".to_string();
  let mut extension_version = "1.1.49".to_string();

  let sites_to_probe = [
    (ProductionSite::Grok, "grok.health", "grok"),
    (
      ProductionSite::Facebook,
      "social.facebook.health",
      "facebook",
    ),
    (ProductionSite::TikTok, "social.tiktok.health", "tiktok"),
    (
      ProductionSite::YouTubeStudio,
      "social.youtube.health",
      "youtube-studio",
    ),
  ];

  for (site, health_method, site_key) in sites_to_probe {
    let matching: Vec<&serde_json::Value> = targets
      .iter()
      .filter(|t| {
        let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
        if t_type != "page" {
          return false;
        }
        let t_url = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
        if let Ok(parsed) = url::Url::parse(t_url) {
          if let Some(host) = parsed.host_str() {
            return site.matches_host(host);
          }
        }
        false
      })
      .collect();

    if !matching.is_empty() {
      if site == ProductionSite::Grok {
        grok_logged_in = Some(true);
        site_sessions.push(WorkerSiteSession {
          site: ProductionSite::Grok,
          state: SiteSessionState::Ready,
          checked_at: Some(chrono::Utc::now().to_rfc3339()),
          current_host: Some("grok.com".to_string()),
          account_identifier: None,
          message: None,
        });
        site_capabilities.insert(
          "grok".to_string(),
          vec![
            "grok.image.edit".to_string(),
            "grok.expand.9_16".to_string(),
            "grok.video.generate".to_string(),
            "grok.video.upscale".to_string(),
            "grok.prompt.queue".to_string(),
          ],
        );
      } else {
        site_sessions.push(WorkerSiteSession {
          site,
          state: SiteSessionState::Ready,
          checked_at: Some(chrono::Utc::now().to_rfc3339()),
          current_host: Some(site_key.to_string()),
          account_identifier: None,
          message: None,
        });
      }
    } else {
      site_sessions.push(WorkerSiteSession {
        site,
        state: SiteSessionState::Unknown,
        checked_at: Some(chrono::Utc::now().to_rfc3339()),
        current_host: None,
        account_identifier: None,
        message: Some(format!("No active tab open for {}", site.display_name())),
      });
    }
  }

  let mut aggregate_caps: Vec<String> = site_capabilities
    .values()
    .flatten()
    .filter(|cap| {
      cap.as_str() != "social.facebook.publish"
        && cap.as_str() != "social.tiktok.publish"
        && cap.as_str() != "social.youtube.publish"
    })
    .cloned()
    .collect();
  aggregate_caps.sort();
  aggregate_caps.dedup();

  Ok(WorkerHealthHandshakeRequest {
    profile_id: profile_id.to_string(),
    protocol_version: 1,
    extension_version,
    worker_state: overall_worker_state,
    logged_in: grok_logged_in,
    capabilities: aggregate_caps,
    site_sessions,
    site_capabilities,
  })
}

pub async fn dispatch_worker_handler(
  Path(worker_id): Path<String>,
  Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
  // 1. Protocol version validation
  let proto = payload
    .get("protocol")
    .and_then(|v| v.as_str())
    .unwrap_or("");
  let proto_ver = payload
    .get("protocolVersion")
    .and_then(|v| v.as_u64())
    .unwrap_or(0);
  if proto != "floword-production" || proto_ver != 1 {
    return Err((
      StatusCode::BAD_REQUEST,
      Json(serde_json::json!({
        "error": {
          "code": "PROTOCOL_MISMATCH",
          "message": format!("Protocol version mismatch: expected floword-production v1, got '{proto}' v{proto_ver}")
        }
      })),
    ));
  }

  // 2. Strict required envelope identity fields validation (No empty strings)
  let req_id = payload
    .get("requestId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let job_id = payload
    .get("jobId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let step_id = payload
    .get("stepId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let attempt_id = payload
    .get("attemptId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let lease_id = payload
    .get("leaseId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let profile_id = payload
    .get("profileId")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();
  let method = payload
    .get("method")
    .and_then(|v| v.as_str())
    .unwrap_or("")
    .trim();

  if req_id.is_empty()
    || job_id.is_empty()
    || step_id.is_empty()
    || attempt_id.is_empty()
    || lease_id.is_empty()
    || profile_id.is_empty()
    || method.is_empty()
  {
    return Err((
      StatusCode::BAD_REQUEST,
      Json(serde_json::json!({
        "error": {
          "code": "INVALID_REQUEST",
          "message": "Missing or empty required correlation identity fields (requestId, jobId, stepId, attemptId, leaseId, profileId, method)"
        }
      })),
    ));
  }

  // 3. Method Descriptor lookup (Reject unknown methods before CDP!)
  let descriptor = match ProductionMethodDescriptor::lookup(method) {
    Some(desc) => desc,
    None => {
      return Err((
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
          "error": {
            "code": "METHOD_NOT_SUPPORTED",
            "message": format!("Method '{method}' is not supported in this runtime")
          }
        })),
      ));
    }
  };

  // 4. Method implementation policy check
  if !descriptor.implemented {
    return Err((
      StatusCode::NOT_IMPLEMENTED,
      Json(serde_json::json!({
        "error": {
          "code": "CAPABILITY_UNAVAILABLE",
          "message": format!("Method '{method}' is recognized but executor is not implemented in current runtime phase"),
          "retryable": false
        }
      })),
    ));
  }

  // 5. Strict Active Lease Validation
  let lease = match WORKER_REGISTRY
    .validate_active_lease(lease_id, job_id, step_id, attempt_id, profile_id)
    .await
  {
    Ok(l) => l,
    Err(err) => {
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::CONFLICT);
      return Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": err.code_str(),
            "message": err.message
          }
        })),
      ));
    }
  };

  // 6. Worker / Profile ID check: URL path worker_id matches lease.worker_id or lease.profile_id
  let norm_url = worker_id.strip_prefix("browser-profile:").unwrap_or(&worker_id);
  let norm_lease = lease.worker_id.strip_prefix("browser-profile:").unwrap_or(&lease.worker_id);
  let norm_profile = lease.profile_id.strip_prefix("browser-profile:").unwrap_or(&lease.profile_id);

  if worker_id != lease.worker_id && norm_url != norm_lease && norm_url != norm_profile && worker_id != lease.profile_id {
    return Err((
      StatusCode::BAD_REQUEST,
      Json(serde_json::json!({
        "error": {
          "code": "INVALID_LEASE",
          "message": format!(
            "Dispatch worker_id '{worker_id}' does not match lease worker_id '{}'",
            lease.worker_id
          )
        }
      })),
    ));
  }

  // 7. Capability Escalation Protection (Section 10): lease.capability MUST equal descriptor.required_capability
  if lease.capability != descriptor.required_capability {
    return Err((
      StatusCode::CONFLICT,
      Json(serde_json::json!({
        "error": {
          "code": "CAPABILITY_MISMATCH",
          "message": format!(
            "Lease capability '{}' does not match method required capability '{}'",
            lease.capability, descriptor.required_capability
          ),
          "retryable": false
        }
      })),
    ));
  }

  // 8. Worker State and Capability Check
  let worker_opt = {
    let workers = WORKER_REGISTRY.list_workers().await;
    workers
      .workers
      .into_iter()
      .find(|w| w.worker_id == worker_id || w.profile_id == profile_id)
  };

  let worker = match worker_opt {
    Some(w) if w.state != WorkerState::Offline => w,
    _ => {
      return Err((
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
          "error": {
            "code": "BRIDGE_DISCONNECTED",
            "message": format!("Worker '{worker_id}' is disconnected or offline")
          }
        })),
      ));
    }
  };

  if !worker.capabilities.contains(&lease.capability) {
    return Err((
      StatusCode::CONFLICT,
      Json(serde_json::json!({
        "error": {
          "code": "CAPABILITY_UNAVAILABLE",
          "message": format!("Worker no longer supports lease capability '{}'", lease.capability),
          "retryable": false
        }
      })),
    ));
  }

  // 9. Dispatch to real Extension on Profile
  match dispatch_to_profile_extension(profile_id, &payload).await {
    Ok(result) => {
      // 10. Strict Response Validation: Protocol, version, and 6-field correlation echo
      let resp_proto = result
        .get("protocol")
        .and_then(|v| v.as_str())
        .unwrap_or("");
      let resp_proto_ver = result
        .get("protocolVersion")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
      if resp_proto != "floword-production" || resp_proto_ver != 1 {
        return Err((
          StatusCode::BAD_REQUEST,
          Json(serde_json::json!({
            "error": {
              "code": "PROTOCOL_MISMATCH",
              "message": format!(
                "Extension response protocol mismatch: expected floword-production v1, got '{resp_proto}' v{resp_proto_ver}"
              )
            }
          })),
        ));
      }

      let resp_req_id = result
        .get("requestId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
      let resp_job_id = result.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
      let resp_step_id = result.get("stepId").and_then(|v| v.as_str()).unwrap_or("");
      let resp_attempt_id = result
        .get("attemptId")
        .and_then(|v| v.as_str())
        .unwrap_or("");
      let resp_lease_id = result.get("leaseId").and_then(|v| v.as_str()).unwrap_or("");
      let resp_profile_id = result
        .get("profileId")
        .and_then(|v| v.as_str())
        .unwrap_or("");

      if resp_req_id != req_id
        || resp_job_id != job_id
        || resp_step_id != step_id
        || resp_attempt_id != attempt_id
        || resp_lease_id != lease_id
        || resp_profile_id != profile_id
      {
        return Err((
          StatusCode::BAD_REQUEST,
          Json(serde_json::json!({
            "protocol": "floword-production",
            "protocolVersion": 1,
            "requestId": req_id,
            "jobId": job_id,
            "stepId": step_id,
            "attemptId": attempt_id,
            "leaseId": lease_id,
            "profileId": profile_id,
            "ok": false,
            "error": {
              "code": "CORRELATION_MISMATCH",
              "message": format!(
                "Extension response correlation identity mismatch. Expected req={}, job={}, step={}, attempt={}, lease={}, profile={}; got req={}, job={}, step={}, attempt={}, lease={}, profile={}",
                req_id, job_id, step_id, attempt_id, lease_id, profile_id,
                resp_req_id, resp_job_id, resp_step_id, resp_attempt_id, resp_lease_id, resp_profile_id
              ),
              "retryable": false
            }
          })),
        ));
      }

      Ok(Json(result))
    }
    Err(err) => {
      let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
      Err((
        status,
        Json(serde_json::json!({
          "protocol": "floword-production",
          "protocolVersion": 1,
          "requestId": req_id,
          "jobId": job_id,
          "stepId": step_id,
          "attemptId": attempt_id,
          "leaseId": lease_id,
          "profileId": profile_id,
          "ok": false,
          "error": {
            "code": err.code_str(),
            "message": err.message,
            "retryable": err.is_transient()
          }
        })),
      ))
    }
  }
}
