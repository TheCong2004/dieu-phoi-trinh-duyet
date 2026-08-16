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
    .route("/v1/workers", get(list_workers_handler))
    .route("/v1/workers/leases", get(list_leases_handler))
}

pub async fn acquire_worker_handler(
  Json(payload): Json<AcquireWorkerRequest>,
) -> Result<Json<AcquireWorkerResponse>, (StatusCode, Json<serde_json::Value>)> {
  match WORKER_REGISTRY.acquire(payload).await {
    Ok(res) => Ok(Json(res)),
    Err((code, msg)) => {
      let status = match code {
        400 => StatusCode::BAD_REQUEST,
        404 => StatusCode::NOT_FOUND,
        409 => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
      };
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": if msg.contains("BUSY") { "WORKER_BUSY" } else { "NO_AVAILABLE_WORKER" },
            "message": msg
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
    Err((code, msg)) => {
      let status = match code {
        400 => StatusCode::BAD_REQUEST,
        404 => StatusCode::NOT_FOUND,
        409 => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
      };
      Err((
        status,
        Json(serde_json::json!({
          "error": {
            "code": "HEARTBEAT_FAILED",
            "message": msg
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

pub async fn list_workers_handler() -> Json<ListWorkersResponse> {
  Json(WORKER_REGISTRY.list_workers().await)
}

pub async fn list_leases_handler() -> Json<ListLeasesResponse> {
  Json(WORKER_REGISTRY.list_leases().await)
}
