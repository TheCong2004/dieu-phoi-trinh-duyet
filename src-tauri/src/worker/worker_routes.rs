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
pub async fn dispatch_to_profile_extension(
  profile_id: &str,
  payload: &serde_json::Value,
) -> Result<serde_json::Value, WorkerError> {
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
  let profile = profiles
    .into_iter()
    .find(|p| p.id.to_string() == profile_id)
    .ok_or_else(|| {
      WorkerError::new(
        WorkerErrorCode::InvalidProfile,
        format!("Profile ID '{profile_id}' not found in runtime"),
      )
    })?;

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

  // 2. Strict grok.com page target selection with multi-tab guard
  let grok_targets: Vec<&serde_json::Value> = targets
    .iter()
    .filter(|t| {
      let t_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("");
      let t_url = t.get("url").and_then(|v| v.as_str()).unwrap_or("");
      t_type == "page" && t_url.contains("grok.com")
    })
    .collect();

  if grok_targets.is_empty() {
    return Err(WorkerError::new(
      WorkerErrorCode::GrokPageNotReady,
      format!(
        "No active grok.com page target found for profile '{profile_id}'. Ensure Grok tab is open."
      ),
    ));
  }

  if grok_targets.len() > 1 {
    return Err(WorkerError::new(
      WorkerErrorCode::GrokTargetAmbiguous,
      format!(
        "Multiple active grok.com tabs ({}) detected for profile '{profile_id}'. Exactly 1 active Grok tab is required for deterministic execution.",
        grok_targets.len()
      ),
    ));
  }

  let target = grok_targets[0];
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

  // 3. Discover Execution Contexts via Runtime.enable
  let enable_req = serde_json::json!({
    "id": 1,
    "method": "Runtime.enable"
  });
  let _ = ws_stream
    .send(Message::Text(enable_req.to_string().into()))
    .await;

  // Drain initial context creation events (up to 500ms timeout)
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
    }} else {{
      false;
    }}"#,
    profile_id
  );

  let mut target_context_id: Option<i64> = None;
  for cid in isolated_context_ids.iter().copied() {
    if let Ok(res) = send_cdp_evaluate(
      &mut ws_stream,
      10 + cid as u64,
      &bind_expr,
      cid,
      Duration::from_secs(5),
    )
    .await
    {
      if res.as_bool() == Some(true) {
        target_context_id = Some(cid);
        break;
      }
    }
  }

  // Fail-closed: Strictly require isolated context. Never evaluate on default page context!
  let target_context_id = target_context_id.ok_or_else(|| {
    WorkerError::new(
      WorkerErrorCode::ExtensionContextNotFound,
      format!(
        "Extension isolated execution context not found on grok.com for profile '{profile_id}'"
      ),
    )
  })?;

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
      if (typeof window.__tobyflowGrokOnMessage === 'function') {{
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

  let prod_result = send_cdp_evaluate(
    &mut ws_stream,
    1001,
    &exec_expr,
    target_context_id,
    method_dur,
  )
  .await?;
  Ok(prod_result)
}

/// Active health probe from Donut Browser to Extension instance with zero fake fallbacks.
pub async fn probe_worker_health(
  profile_id: &str,
) -> Result<WorkerHealthHandshakeRequest, WorkerError> {
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
    "params": {
      "timeoutMs": 10000
    },
    "createdAt": chrono::Utc::now().to_rfc3339()
  });

  let res = dispatch_to_profile_extension(profile_id, &health_req).await?;

  let proto = res.get("protocol").and_then(|v| v.as_str()).unwrap_or("");
  let proto_ver = res
    .get("protocolVersion")
    .and_then(|v| v.as_u64())
    .unwrap_or(0);
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

  let resp_profile = health_val
    .get("profileId")
    .and_then(|v| v.as_str())
    .unwrap_or("");
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
  let profile_id = payload
    .get("profileId")
    .and_then(|v| v.as_str())
    .unwrap_or_default();
  let req_id = payload
    .get("requestId")
    .and_then(|v| v.as_str())
    .unwrap_or("");
  let job_id = payload.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
  let step_id = payload.get("stepId").and_then(|v| v.as_str()).unwrap_or("");
  let attempt_id = payload
    .get("attemptId")
    .and_then(|v| v.as_str())
    .unwrap_or("");
  let lease_id = payload
    .get("leaseId")
    .and_then(|v| v.as_str())
    .unwrap_or("");

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
  if let Err(err) = WORKER_REGISTRY
    .validate_active_lease(lease_id, job_id, step_id, attempt_id, profile_id)
    .await
  {
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
    workers.workers.iter().any(|w| {
      (w.worker_id == worker_id || w.profile_id == profile_id) && w.state != WorkerState::Offline
    })
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
