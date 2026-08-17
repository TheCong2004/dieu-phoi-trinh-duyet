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
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

pub fn worker_routes() -> Router {
  Router::new()
    .route("/v1/workers/acquire", post(acquire_worker_handler))
    .route(
      "/v1/workers/leases/:lease_id/heartbeat",
      post(heartbeat_lease_handler),
    )
    .route(
      "/v1/workers/leases/:lease_id/release",
      post(release_lease_handler),
    )
    .route(
      "/v1/workers/:worker_id/reconcile",
      post(reconcile_worker_handler),
    )
    .route(
      "/v1/workers/:worker_id/health",
      post(worker_health_handshake_handler),
    )
    .route(
      "/v1/workers/:worker_id/dispatch",
      post(dispatch_worker_handler),
    )
    .route("/v1/workers", get(list_workers_handler))
    .route("/v1/workers/leases", get(list_leases_handler))
}

pub async fn acquire_worker_handler(
  Json(payload): Json<AcquireWorkerRequest>,
) -> Result<Json<AcquireWorkerResponse>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY.acquire(payload).await {
    Ok(res) => Ok(Json(res)),
    Err(err) => {
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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

pub async fn release_lease_handler(
  Path(lease_id): Path<String>,
) -> impl IntoResponse {
  let res = WORKER_REGISTRY.release(&lease_id).await;

  // Trigger background health probe after release to reconcile to Ready if IDLE
  let leases = WORKER_REGISTRY.list_leases().await;
  if let Some(lease) = leases.leases.iter().find(|l| l.lease_id == lease_id) {
    let profile_id = lease.profile_id.clone();
    let worker_id = lease.worker_id.clone();
    tauri::async_runtime::spawn(async move {
      if let Ok(handshake) = probe_worker_health(&profile_id).await {
        let _ = WORKER_REGISTRY.handle_health_handshake(&worker_id, handshake).await;
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
    .reconcile_worker(&worker_id, payload.is_idle, payload.is_healthy, payload.grok_logged_in)
    .await
  {
    Ok(state) => Ok(Json(serde_json::json!({
      "worker_id": worker_id,
      "state": state
    }))),
    Err(err) => {
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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
  match WORKER_REGISTRY.handle_health_handshake(&worker_id, payload).await {
    Ok(()) => Ok(Json(serde_json::json!({
      "worker_id": worker_id,
      "status": "HANDSHAKE_ACCEPTED"
    }))),
    Err(err) => {
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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

/// Dispatches a Floword production command to the exact target browser profile extension via CDP.
pub async fn dispatch_to_profile_extension(
  profile_id: &str,
  payload: &serde_json::Value,
) -> Result<serde_json::Value, WorkerError> {
  let profiles_dir = crate::profile::ProfileManager::instance().get_profiles_dir();
  let profiles = crate::profile::ProfileManager::instance()
    .list_profiles()
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Failed to list profiles: {e}")))?;

  // 1. Strict profile ID match (no display name fallback)
  let profile = profiles
    .into_iter()
    .find(|p| p.id == profile_id)
    .ok_or_else(|| WorkerError::new(WorkerErrorCode::InvalidProfile, format!("Profile ID '{profile_id}' not found in runtime")))?;

  let profile_path = profile.get_profile_data_path(&profiles_dir);
  let profile_path_str = profile_path.to_string_lossy();

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
    .timeout(std::time::Duration::from_secs(5))
    .build()
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, e.to_string()))?;

  let targets_resp = http_client
    .get(&url)
    .send()
    .await
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("CDP endpoint unavailable: {e}")))?;

  let targets: Vec<serde_json::Value> = targets_resp
    .json()
    .await
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Failed to parse CDP targets: {e}")))?;

  // 2. Strict grok.com page target selection (never evaluate on arbitrary pages)
  let target = targets
    .iter()
    .find(|t| {
      let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
      let t_url = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
      t_type == "page" && t_url.contains("grok.com")
    })
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::GrokPageNotReady,
        format!("No active grok.com page target found for profile '{profile_id}'. Ensure Grok tab is open."),
      )
    })?;

  let ws_url = target
    .get("webSocketDebuggerUrl")
    .and_then(|v| v.as_str())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        "No webSocketDebuggerUrl in target".to_string(),
      )
    })?;

  let (mut ws_stream, _) = connect_async(ws_url)
    .await
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Failed to connect to CDP WS: {e}")))?;

  let payload_json_str = serde_json::to_string(payload).map_err(|e| {
    WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Failed to serialize payload: {e}"))
  })?;

  // 3. Cross-World postMessage bridge with timeout and fallback
  let expression = format!(
    r#"new Promise((resolve, reject) => {{
      const req = {payload_json_str};
      const replyId = 'FLOWORD_' + Math.random().toString(36).slice(2);
      let handled = false;

      const handler = (e) => {{
        if (e.data && e.data.protocol === 'floword-production-reply' && e.data.__flowordReplyId === replyId) {{
          handled = true;
          window.removeEventListener('message', handler);
          clearTimeout(timer);
          resolve(e.data.result);
        }}
      }};
      window.addEventListener('message', handler);

      const timer = setTimeout(() => {{
        if (!handled) {{
          window.removeEventListener('message', handler);
          reject(new Error('Extension response timeout from isolated world'));
        }}
      }}, 180000);

      window.postMessage({{ ...req, __flowordReplyId: replyId }}, '*');

      if (typeof window.__tobyflowGrokOnMessage === 'function') {{
        window.__tobyflowGrokOnMessage(req, null, (res) => {{
          if (!handled) {{
            handled = true;
            window.removeEventListener('message', handler);
            clearTimeout(timer);
            resolve(res);
          }}
        }});
      }} else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendMessage) {{
        chrome.runtime.sendMessage(req, (res) => {{
          if (!handled && !chrome.runtime.lastError) {{
            handled = true;
            window.removeEventListener('message', handler);
            clearTimeout(timer);
            resolve(res);
          }}
        }});
      }}
    }})"#
  );

  let cdp_req = serde_json::json!({
    "id": 1001,
    "method": "Runtime.evaluate",
    "params": {
      "expression": expression,
      "awaitPromise": true,
      "returnByValue": true
    }
  });

  ws_stream
    .send(Message::Text(cdp_req.to_string().into()))
    .await
    .map_err(|e| WorkerError::new(WorkerErrorCode::BridgeDisconnected, format!("Failed to send CDP command: {e}")))?;

  while let Some(msg) = ws_stream.next().await {
    match msg {
      Ok(Message::Text(text)) => {
        if let Ok(resp) = serde_json::from_str::<serde_json::Value>(text.as_str()) {
          if resp.get("id") == Some(&serde_json::json!(1001)) {
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

            // 4. Extract result.result.value from CDP RemoteObject wrapper
            let prod_result = resp
              .get("result")
              .and_then(|r| r.get("result"))
              .and_then(|r| r.get("value"))
              .cloned()
              .or_else(|| {
                resp
                  .get("result")
                  .and_then(|r| r.get("value"))
                  .cloned()
              })
              .ok_or_else(|| {
                WorkerError::new(
                  WorkerErrorCode::BridgeDisconnected,
                  "Failed to extract ProductionResult value from CDP evaluate response",
                )
              })?;

            return Ok(prod_result);
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
    "No result received from extension via CDP",
  ))
}

/// Active health probe from Donut Browser to Extension instance with zero fake fallbacks.
pub async fn probe_worker_health(profile_id: &str) -> Result<WorkerHealthHandshakeRequest, WorkerError> {
  let health_req = serde_json::json!({
    "protocol": "floword-production",
    "protocolVersion": 1,
    "requestId": format!("HEALTH_PROBE_{}", Uuid::new_v4().simple()),
    "jobId": "SYS",
    "stepId": "HEALTH_PROBE",
    "attemptId": "1",
    "leaseId": "SYS",
    "profileId": profile_id,
    "method": "grok.health",
    "params": {},
    "createdAt": chrono::Utc::now().to_rfc3339()
  });

  let res = dispatch_to_profile_extension(profile_id, &health_req).await?;

  let proto = res.get("protocol").and_then(|v| v.as_str()).unwrap_or("");
  let proto_ver = res.get("protocolVersion").and_then(|v| v.as_u64()).unwrap_or(0);
  if proto != "floword-production" || proto_ver != 1 {
    return Err(WorkerError::new(
      WorkerErrorCode::ProtocolMismatch,
      format!("Health probe protocol mismatch: expected floword-production v1, got '{proto}' v{proto_ver}"),
    ));
  }

  let health_val = res.get("result").ok_or_else(|| {
    WorkerError::new(
      WorkerErrorCode::InvalidHealthResponse,
      "Health response missing 'result' object",
    )
  })?;

  let resp_profile = health_val.get("profileId").and_then(|v| v.as_str()).unwrap_or("");
  if resp_profile != profile_id {
    return Err(WorkerError::new(
      WorkerErrorCode::InvalidProfile,
      format!("Health probe profile mismatch: expected {profile_id}, got {resp_profile}"),
    ));
  }

  let logged_in = health_val
    .get("loggedIn")
    .and_then(|v| v.as_bool())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidHealthResponse,
        "Health response missing required boolean 'loggedIn'",
      )
    })?;

  let worker_state = health_val
    .get("workerState")
    .and_then(|v| v.as_str())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidHealthResponse,
        "Health response missing required string 'workerState'",
      )
    })?
    .to_string();

  let ext_ver = health_val
    .get("extensionVersion")
    .and_then(|v| v.as_str())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidHealthResponse,
        "Health response missing required string 'extensionVersion'",
      )
    })?
    .to_string();

  let caps_arr = health_val
    .get("capabilities")
    .and_then(|v| v.as_array())
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidHealthResponse,
        "Health response missing required array 'capabilities'",
      )
    })?;

  if caps_arr.is_empty() {
    return Err(WorkerError::new(
      WorkerErrorCode::InvalidHealthResponse,
      "Health response capabilities array cannot be empty",
    ));
  }

  let capabilities: Vec<String> = caps_arr
    .iter()
    .filter_map(|c| c.as_str().map(|s| s.to_string()))
    .collect();

  Ok(WorkerHealthHandshakeRequest {
    profile_id: profile_id.to_string(),
    protocol_version: 1,
    extension_version: ext_ver,
    worker_state,
    logged_in,
    capabilities,
  })
}

pub async fn dispatch_worker_handler(
  Path(worker_id): Path<String>,
  Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
  let profile_id = payload.get("profileId").and_then(|v| v.as_str()).unwrap_or_default();
  let req_id = payload.get("requestId").and_then(|v| v.as_str()).unwrap_or("");
  let job_id = payload.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
  let step_id = payload.get("stepId").and_then(|v| v.as_str()).unwrap_or("");
  let attempt_id = payload.get("attemptId").and_then(|v| v.as_str()).unwrap_or("");
  let lease_id = payload.get("leaseId").and_then(|v| v.as_str()).unwrap_or("");

  // 1. Exact profile identity check
  if worker_id != profile_id && worker_id != format!("browser-profile:{profile_id}") {
    return Err((
      StatusCode::BAD_REQUEST,
      Json(serde_json::json!({
        "error": {
          "code": "INVALID_PROFILE",
          "message": format!("Target worker '{worker_id}' does not match request profileId '{profile_id}'")
        }
      })),
    ));
  }

  // 2. Strict Active Lease Validation (Two-way Lease <-> Worker cross check)
  if let Err(err) = WORKER_REGISTRY.validate_active_lease(lease_id, job_id, step_id, attempt_id, profile_id).await {
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

  // 3. Worker State Check
  let is_available = {
    let workers = WORKER_REGISTRY.list_workers().await;
    workers
      .workers
      .iter()
      .any(|w| (w.worker_id == worker_id || w.profile_id == profile_id) && w.state != WorkerState::Offline)
  };

  if !is_available {
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

  // 4. Dispatch to real Extension on Profile
  match dispatch_to_profile_extension(profile_id, &payload).await {
    Ok(result) => Ok(Json(result)),
    Err(err) => {
      let status = StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
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
            "retryable": true
          }
        })),
      ))
    }
  }
}
