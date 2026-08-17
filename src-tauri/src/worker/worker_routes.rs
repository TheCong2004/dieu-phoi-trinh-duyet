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
