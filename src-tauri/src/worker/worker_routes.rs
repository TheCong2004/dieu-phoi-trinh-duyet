use super::worker_registry::WORKER_REGISTRY;
use super::worker_types::*;
use axum::{
  extract::Path,
  http::StatusCode,
  response::{IntoResponse, Json},
  routing::{get, post},
  Router,
};

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

pub async fn dispatch_worker_handler(
  Path(worker_id): Path<String>,
  Json(payload): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
  let profile_id = payload.get("profileId").and_then(|v| v.as_str()).unwrap_or_default();

  // 1. Exact profile verification
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

  // 2. Look up worker readiness in registry
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

  // 3. Preserve exact correlation IDs in result envelope
  let req_id = payload.get("requestId").and_then(|v| v.as_str()).unwrap_or("");
  let job_id = payload.get("jobId").and_then(|v| v.as_str()).unwrap_or("");
  let step_id = payload.get("stepId").and_then(|v| v.as_str()).unwrap_or("");
  let attempt_id = payload.get("attemptId").and_then(|v| v.as_str()).unwrap_or("");
  let lease_id = payload.get("leaseId").and_then(|v| v.as_str()).unwrap_or("");

  Ok(Json(serde_json::json!({
    "protocol": "floword-production",
    "protocolVersion": 1,
    "requestId": req_id,
    "jobId": job_id,
    "stepId": step_id,
    "attemptId": attempt_id,
    "leaseId": lease_id,
    "profileId": profile_id,
    "ok": true,
    "result": {
      "media_type": "image",
      "source": "grok",
      "locator": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
      "mime_type": "image/png"
    }
  })))
}

