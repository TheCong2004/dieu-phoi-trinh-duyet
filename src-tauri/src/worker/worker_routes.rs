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

  let profile = profiles
    .into_iter()
    .find(|p| p.id == profile_id || p.name == profile_id)
    .ok_or_else(|| WorkerError::new(WorkerErrorCode::InvalidProfile, format!("Profile '{profile_id}' not found in runtime")))?;

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

  // Pick target: prefer grok / toby page target, fallback to any active page target
  let target = targets
    .iter()
    .find(|t| {
      let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
      let t_url = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
      t_type == "page" && (t_url.contains("grok.com") || t_url.contains("labs.toby.vn"))
    })
    .or_else(|| {
      targets
        .iter()
        .find(|t| t.get("type").and_then(|v| v.as_str()) == Some("page"))
    })
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::BridgeDisconnected,
        format!("No usable page target found for profile '{profile_id}'"),
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

  let expression = format!(
    r#"new Promise((resolve, reject) => {{
      const req = {payload_json_str};
      if (typeof window.__tobyflowGrokOnMessage === 'function') {{
        window.__tobyflowGrokOnMessage(req, null, resolve);
      }} else if (typeof chrome !== 'undefined' && chrome.runtime && chrome.runtime.sendMessage) {{
        chrome.runtime.sendMessage(req, resp => {{
          if (chrome.runtime.lastError) {{
            reject(new Error(chrome.runtime.lastError.message));
          }} else {{
            resolve(resp);
          }}
        }});
      }} else {{
        reject(new Error('Extension not loaded in active page target'));
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
            if let Some(val) = resp.get("result").and_then(|r| r.get("value")) {
              return Ok(val.clone());
            }
            return Ok(resp.get("result").cloned().unwrap_or(serde_json::json!({})));
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

/// Active health probe from Donut Browser to Extension instance.
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
  let health_val = res.get("result").unwrap_or(&res);

  let logged_in = health_val.get("loggedIn").and_then(|v| v.as_bool()).unwrap_or(false);
  let worker_state = health_val.get("workerState").and_then(|v| v.as_str()).unwrap_or("IDLE").to_string();
  let ext_ver = health_val.get("extensionVersion").and_then(|v| v.as_str()).unwrap_or("1.1.49").to_string();
  let caps = health_val
    .get("capabilities")
    .and_then(|v| v.as_array())
    .map(|arr| arr.iter().filter_map(|c| c.as_str().map(|s| s.to_string())).collect())
    .unwrap_or_else(|| vec!["grok.image.edit".to_string()]);

  Ok(WorkerHealthHandshakeRequest {
    profile_id: profile_id.to_string(),
    protocol_version: 1,
    extension_version: ext_ver,
    worker_state,
    logged_in,
    capabilities: caps,
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

  // 2. Strict Active Lease Validation
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
