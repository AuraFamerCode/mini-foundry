//! Concurrent control-plane API backed by PostgreSQL.
//!
//! This is an initial service slice, not a migration of the legacy Node app.
//! PostgreSQL is the source of truth; each dataset commit serializes on its
//! metadata row and atomically publishes both materialized rows and a delta.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use ontology_core::{
    canonical_primary_key_token, validate_object_type, Column, DataType, DatasetSchema, ObjectType,
    Property, Row,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    types::Json as SqlJson,
    Postgres, QueryBuilder, Row as SqlRow, Transaction,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

const MAX_PAGE_SIZE: i64 = 1_000;
const DEFAULT_PAGE_SIZE: i64 = 100;
const MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub max_db_connections: u32,
    pub max_concurrent_requests: usize,
    pub db_acquire_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let bind = env_or("BIND_ADDR", "127.0.0.1:6475")
            .parse()
            .map_err(|_| ConfigError("BIND_ADDR must be a valid socket address"))?;
        let database_url =
            std::env::var("DATABASE_URL").map_err(|_| ConfigError("DATABASE_URL is required"))?;
        if database_url.trim().is_empty() {
            return Err(ConfigError("DATABASE_URL cannot be empty"));
        }
        let max_db_connections = parse_positive("MAX_DB_CONNECTIONS", 32_u32)?;
        let max_concurrent_requests = parse_positive("MAX_CONCURRENT_REQUESTS", 256_usize)?;
        let db_acquire_timeout_ms = parse_positive("DB_ACQUIRE_TIMEOUT_MS", 5_000_u64)?;
        Ok(Self {
            bind,
            database_url,
            max_db_connections,
            max_concurrent_requests,
            db_acquire_timeout: Duration::from_millis(db_acquire_timeout_ms),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConfigError(&'static str);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for ConfigError {}

fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn parse_positive<T>(name: &'static str, fallback: T) -> Result<T, ConfigError>
where
    T: FromStr + PartialOrd + From<u8>,
{
    let value = match std::env::var(name) {
        Ok(value) => value.parse::<T>().map_err(|_| ConfigError(name))?,
        Err(_) => fallback,
    };
    if value <= T::from(0) {
        return Err(ConfigError(name));
    }
    Ok(value)
}

#[derive(Clone)]
pub struct RequestLimiter {
    semaphore: Arc<Semaphore>,
}

impl RequestLimiter {
    pub fn new(max_concurrent_requests: usize) -> Result<Self, ConfigError> {
        if max_concurrent_requests == 0 {
            return Err(ConfigError("MAX_CONCURRENT_REQUESTS must be positive"));
        }
        Ok(Self {
            semaphore: Arc::new(Semaphore::new(max_concurrent_requests)),
        })
    }

    fn try_enter(&self) -> Result<OwnedSemaphorePermit, ()> {
        self.semaphore.clone().try_acquire_owned().map_err(|_| ())
    }
}

#[derive(Clone)]
pub struct AppState {
    pool: PgPool,
    limiter: RequestLimiter,
}

impl AppState {
    pub fn new(pool: PgPool, max_concurrent_requests: usize) -> Result<Self, ConfigError> {
        Ok(Self {
            pool,
            limiter: RequestLimiter::new(max_concurrent_requests)?,
        })
    }
}

pub async fn connect(config: &Config) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(config.max_db_connections)
        .acquire_timeout(config.db_acquire_timeout)
        .connect(&config.database_url)
        .await
}

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    sqlx::migrate!("./migrations").run(pool).await
}

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/datasets", get(list_datasets).post(create_dataset))
        .route("/v1/datasets/:dataset", get(get_dataset))
        .route("/v1/datasets/:dataset/rows", get(get_rows))
        .route(
            "/v1/datasets/:dataset/transactions",
            axum::routing::post(commit_dataset_transaction),
        )
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            bound_concurrency,
        ))
        .with_state(state)
}

async fn bound_concurrency(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Ok(_permit) = state.limiter.try_enter() else {
        return ApiError::overloaded().into_response();
    };
    next.run(request).await
}

async fn live() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn ready(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .map_err(ApiError::database)?;
    Ok(Json(json!({"ok": true, "database": "ready"})))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApiDataType {
    String,
    Integer,
    Number,
    Boolean,
    Date,
    Timestamp,
    Json,
}

impl ApiDataType {
    fn core_type(&self) -> DataType {
        match self {
            Self::String => DataType::String,
            Self::Integer => DataType::Integer,
            Self::Number => DataType::Number,
            Self::Boolean => DataType::Boolean,
            Self::Date => DataType::Date,
            Self::Timestamp => DataType::Timestamp,
            Self::Json => DataType::Json,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ColumnDef {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: ApiDataType,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateDatasetRequest {
    name: String,
    schema: Vec<ColumnDef>,
    primary_key: String,
}

#[derive(Debug, Serialize)]
struct DatasetMetadata {
    id: Uuid,
    name: String,
    schema: Vec<ColumnDef>,
    primary_key: String,
    version: i64,
    row_count: i64,
    created_at: String,
}

#[derive(Debug, Serialize)]
struct DatasetSummary {
    id: Uuid,
    name: String,
    primary_key: String,
    version: i64,
    row_count: i64,
}

async fn create_dataset(
    State(state): State<AppState>,
    Json(request): Json<CreateDatasetRequest>,
) -> Result<(StatusCode, Json<DatasetMetadata>), ApiError> {
    validate_schema(&request.name, &request.schema, &request.primary_key)?;
    let id = Uuid::new_v4();
    let schema = serde_json::to_value(&request.schema).map_err(ApiError::internal)?;
    let row = sqlx::query(
        "INSERT INTO datasets (id, name, schema_json, primary_key) \
         VALUES ($1, $2, $3, $4) \
         RETURNING id, name, schema_json, primary_key, version, row_count, created_at::text AS created_at",
    )
    .bind(id)
    .bind(request.name)
    .bind(SqlJson(schema))
    .bind(request.primary_key)
    .fetch_one(&state.pool)
    .await
    .map_err(ApiError::database)?;
    Ok((StatusCode::CREATED, Json(metadata_from_row(&row)?)))
}

async fn list_datasets(
    State(state): State<AppState>,
) -> Result<Json<Vec<DatasetSummary>>, ApiError> {
    let rows = sqlx::query(
        "SELECT id, name, primary_key, version, row_count \
         FROM datasets ORDER BY created_at, id",
    )
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::database)?;
    let datasets = rows
        .iter()
        .map(|row| {
            Ok(DatasetSummary {
                id: row.try_get("id").map_err(ApiError::internal)?,
                name: row.try_get("name").map_err(ApiError::internal)?,
                primary_key: row.try_get("primary_key").map_err(ApiError::internal)?,
                version: row.try_get("version").map_err(ApiError::internal)?,
                row_count: row.try_get("row_count").map_err(ApiError::internal)?,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(datasets))
}

async fn get_dataset(
    State(state): State<AppState>,
    Path(selector): Path<String>,
) -> Result<Json<DatasetMetadata>, ApiError> {
    let row = select_metadata(&state.pool, &selector).await?;
    Ok(Json(metadata_from_row(&row)?))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RowsQuery {
    limit: Option<i64>,
    after_key: Option<String>,
}

#[derive(Debug, Serialize)]
struct RowsPage {
    dataset_id: Uuid,
    version: i64,
    rows: Vec<Value>,
    next_after_key: Option<String>,
}

async fn get_rows(
    State(state): State<AppState>,
    Path(selector): Path<String>,
    Query(query): Query<RowsQuery>,
) -> Result<Json<RowsPage>, ApiError> {
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_SIZE);
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(ApiError::bad_request(
            "BAD_PAGE_SIZE",
            format!("limit must be between 1 and {MAX_PAGE_SIZE}"),
        ));
    }
    let metadata = select_metadata(&state.pool, &selector).await?;
    let dataset_id: Uuid = metadata.try_get("id").map_err(ApiError::internal)?;
    let version: i64 = metadata.try_get("version").map_err(ApiError::internal)?;
    let fetch_limit = limit + 1;
    let rows = sqlx::query(
        "SELECT row_key, row_json FROM dataset_rows \
         WHERE dataset_id = $1 AND ($2::text IS NULL OR row_key > $2) \
         ORDER BY row_key LIMIT $3",
    )
    .bind(dataset_id)
    .bind(query.after_key)
    .bind(fetch_limit)
    .fetch_all(&state.pool)
    .await
    .map_err(ApiError::database)?;
    let has_more = rows.len() as i64 > limit;
    let visible = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let mut values = Vec::with_capacity(visible.len());
    let mut cursor = None;
    for row in visible {
        cursor = Some(
            row.try_get::<String, _>("row_key")
                .map_err(ApiError::internal)?,
        );
        let value: SqlJson<Value> = row.try_get("row_json").map_err(ApiError::internal)?;
        values.push(value.0);
    }
    Ok(Json(RowsPage {
        dataset_id,
        version,
        rows: values,
        next_after_key: if has_more { cursor } else { None },
    }))
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TransactionOperation {
    Snapshot,
    Append,
    Delete,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitDatasetRequest {
    expected_version: i64,
    operation: TransactionOperation,
    rows: Vec<Value>,
}

#[derive(Debug, Serialize)]
struct CommitResult {
    dataset_id: Uuid,
    version: i64,
    row_count: i64,
    rows_affected: u64,
}

async fn commit_dataset_transaction(
    State(state): State<AppState>,
    Path(selector): Path<String>,
    Json(request): Json<CommitDatasetRequest>,
) -> Result<Json<CommitResult>, ApiError> {
    if request.expected_version < 0 {
        return Err(ApiError::bad_request(
            "BAD_VERSION",
            "expected_version must be zero or greater",
        ));
    }
    if request.rows.is_empty() && request.operation != TransactionOperation::Snapshot {
        return Err(ApiError::bad_request(
            "EMPTY_DELTA",
            "append and delete transactions require at least one row",
        ));
    }

    let mut transaction = state.pool.begin().await.map_err(ApiError::database)?;
    let metadata = select_metadata_for_update(&mut transaction, &selector).await?;
    let version: i64 = metadata.try_get("version").map_err(ApiError::internal)?;
    if version != request.expected_version {
        return Err(ApiError::conflict(
            "VERSION_CONFLICT",
            format!(
                "dataset version is {version}, not {}",
                request.expected_version
            ),
        ));
    }
    let dataset_id: Uuid = metadata.try_get("id").map_err(ApiError::internal)?;
    let name: String = metadata.try_get("name").map_err(ApiError::internal)?;
    let primary_key: String = metadata
        .try_get("primary_key")
        .map_err(ApiError::internal)?;
    let schema_value: SqlJson<Value> = metadata
        .try_get("schema_json")
        .map_err(ApiError::internal)?;
    let schema: Vec<ColumnDef> = serde_json::from_value(schema_value.0)
        .map_err(|_| ApiError::internal("stored dataset schema is invalid"))?;
    let batch = validate_rows(&name, &schema, &primary_key, &request.rows)?;

    let previous_count: i64 = metadata.try_get("row_count").map_err(ApiError::internal)?;
    let (rows_affected, row_count) = match request.operation {
        TransactionOperation::Snapshot => {
            sqlx::query("DELETE FROM dataset_rows WHERE dataset_id = $1")
                .bind(dataset_id)
                .execute(&mut *transaction)
                .await
                .map_err(ApiError::database)?;
            insert_rows(&mut transaction, dataset_id, &batch, false).await?;
            let count = i64::try_from(batch.len())
                .map_err(|_| ApiError::internal("snapshot row count exceeds supported range"))?;
            (batch.len() as u64, count)
        }
        TransactionOperation::Append => {
            let keys = batch.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
            let existing: BTreeSet<String> = sqlx::query_scalar(
                "SELECT row_key FROM dataset_rows WHERE dataset_id = $1 AND row_key = ANY($2)",
            )
            .bind(dataset_id)
            .bind(&keys)
            .fetch_all(&mut *transaction)
            .await
            .map_err(ApiError::database)?
            .into_iter()
            .collect();
            let inserted_count = batch
                .iter()
                .filter(|row| !existing.contains(&row.key))
                .count();
            let result = insert_rows(&mut transaction, dataset_id, &batch, true).await?;
            let added = i64::try_from(inserted_count)
                .map_err(|_| ApiError::internal("append row count exceeds supported range"))?;
            let count = previous_count
                .checked_add(added)
                .ok_or_else(|| ApiError::internal("dataset row count overflow"))?;
            (result, count)
        }
        TransactionOperation::Delete => {
            let keys = batch.iter().map(|row| row.key.clone()).collect::<Vec<_>>();
            let result =
                sqlx::query("DELETE FROM dataset_rows WHERE dataset_id = $1 AND row_key = ANY($2)")
                    .bind(dataset_id)
                    .bind(keys)
                    .execute(&mut *transaction)
                    .await
                    .map_err(ApiError::database)?;
            let removed = i64::try_from(result.rows_affected())
                .map_err(|_| ApiError::internal("delete count exceeds supported range"))?;
            let count = previous_count
                .checked_sub(removed)
                .ok_or_else(|| ApiError::internal("dataset row count underflow"))?;
            (result.rows_affected(), count)
        }
    };

    let next_version = version
        .checked_add(1)
        .ok_or_else(|| ApiError::internal("dataset version overflow"))?;
    let operation = request.operation.as_db_str();
    let delta = match request.operation {
        TransactionOperation::Delete => Value::Array(
            batch
                .iter()
                .map(|row| json!({"row_key": row.key}))
                .collect(),
        ),
        _ => Value::Array(request.rows),
    };
    sqlx::query("UPDATE datasets SET version = $2, row_count = $3 WHERE id = $1")
        .bind(dataset_id)
        .bind(next_version)
        .bind(row_count)
        .execute(&mut *transaction)
        .await
        .map_err(ApiError::database)?;
    sqlx::query(
        "INSERT INTO dataset_transactions (dataset_id, version, operation, delta_json, rows_affected) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(dataset_id)
    .bind(next_version)
    .bind(operation)
    .bind(SqlJson(delta))
    .bind(i64::try_from(rows_affected).map_err(|_| ApiError::internal("row change count overflow"))?)
    .execute(&mut *transaction)
    .await
    .map_err(ApiError::database)?;
    transaction.commit().await.map_err(ApiError::database)?;

    Ok(Json(CommitResult {
        dataset_id,
        version: next_version,
        row_count,
        rows_affected,
    }))
}

impl TransactionOperation {
    fn as_db_str(self) -> &'static str {
        match self {
            Self::Snapshot => "snapshot",
            Self::Append => "append",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug)]
struct ValidatedRow {
    key: String,
    value: Value,
}

fn validate_schema(name: &str, schema: &[ColumnDef], primary_key: &str) -> Result<(), ApiError> {
    let dataset = DatasetSchema {
        name: name.to_owned(),
        columns: schema
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                data_type: column.data_type.core_type(),
            })
            .collect(),
        rows: Vec::new(),
    };
    let object_type = object_type_for(&dataset, primary_key);
    validate_object_type(&object_type, Some(&dataset)).map_err(ApiError::model)
}

fn object_type_for(dataset: &DatasetSchema, primary_key: &str) -> ObjectType {
    ObjectType {
        name: dataset.name.clone(),
        backing_dataset: Some(dataset.name.clone()),
        primary_key: primary_key.to_owned(),
        title_key: primary_key.to_owned(),
        properties: dataset
            .columns
            .iter()
            .map(|column| Property {
                name: column.name.clone(),
                data_type: column.data_type,
                backing_column: Some(column.name.clone()),
            })
            .collect(),
    }
}

fn validate_rows(
    name: &str,
    schema: &[ColumnDef],
    primary_key: &str,
    input: &[Value],
) -> Result<Vec<ValidatedRow>, ApiError> {
    let dataset_schema = DatasetSchema {
        name: name.to_owned(),
        columns: schema
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                data_type: column.data_type.core_type(),
            })
            .collect(),
        rows: input
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let object = value.as_object().ok_or_else(|| {
                    ApiError::bad_request(
                        "BAD_ROW",
                        format!("row {} must be a JSON object", index + 1),
                    )
                })?;
                let mut values = BTreeMap::new();
                for (key, cell) in object {
                    let column = schema
                        .iter()
                        .find(|column| column.name == *key)
                        .ok_or_else(|| {
                            ApiError::bad_request(
                                "BAD_ROW",
                                format!("row {} contains undeclared field '{key}'", index + 1),
                            )
                        })?;
                    if cell.is_null() {
                        continue;
                    }
                    let text = canonical_cell(cell, &column.data_type).ok_or_else(|| {
                        ApiError::bad_request(
                            "BAD_ROW_TYPE",
                            format!("row {} field '{}' has the wrong JSON type", index + 1, key),
                        )
                    })?;
                    values.insert(key.clone(), text);
                }
                Ok(Row { values })
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
    };
    let object_type = object_type_for(&dataset_schema, primary_key);
    validate_object_type(&object_type, Some(&dataset_schema)).map_err(ApiError::model)?;

    input
        .iter()
        .zip(dataset_schema.rows.iter())
        .map(|(value, row)| {
            let key_text = row.values.get(primary_key).ok_or_else(|| {
                ApiError::bad_request(
                    "MISSING_PRIMARY_KEY",
                    format!("field '{primary_key}' is required and cannot be null"),
                )
            })?;
            let key_type = schema
                .iter()
                .find(|column| column.name == primary_key)
                .ok_or_else(|| ApiError::internal("validated key column is missing"))?
                .data_type
                .core_type();
            let key = canonical_primary_key_token(key_text, key_type).map_err(ApiError::model)?;
            Ok(ValidatedRow {
                key,
                value: value.clone(),
            })
        })
        .collect()
}

fn canonical_cell(value: &Value, data_type: &ApiDataType) -> Option<String> {
    match data_type {
        ApiDataType::String | ApiDataType::Date | ApiDataType::Timestamp => {
            value.as_str().map(str::to_owned)
        }
        ApiDataType::Integer => value.as_i64().map(|number| number.to_string()),
        ApiDataType::Number => value.as_number().map(ToString::to_string),
        ApiDataType::Boolean => value.as_bool().map(|boolean| boolean.to_string()),
        ApiDataType::Json => Some(value.to_string()),
    }
}

async fn insert_rows(
    transaction: &mut Transaction<'_, Postgres>,
    dataset_id: Uuid,
    rows: &[ValidatedRow],
    upsert: bool,
) -> Result<u64, ApiError> {
    if rows.is_empty() {
        return Ok(0);
    }
    let mut query =
        QueryBuilder::<Postgres>::new("INSERT INTO dataset_rows (dataset_id, row_key, row_json) ");
    query.push_values(rows.iter(), |mut values, row| {
        values
            .push_bind(dataset_id)
            .push_bind(&row.key)
            .push_bind(SqlJson(row.value.clone()));
    });
    if upsert {
        query.push(
            " ON CONFLICT (dataset_id, row_key) DO UPDATE \
             SET row_json = EXCLUDED.row_json \
             WHERE dataset_rows.row_json IS DISTINCT FROM EXCLUDED.row_json",
        );
    }
    let result = query
        .build()
        .execute(&mut **transaction)
        .await
        .map_err(ApiError::database)?;
    Ok(result.rows_affected())
}

async fn select_metadata(pool: &PgPool, selector: &str) -> Result<sqlx::postgres::PgRow, ApiError> {
    let query = if let Ok(id) = Uuid::parse_str(selector) {
        sqlx::query(
            "SELECT id, name, schema_json, primary_key, version, row_count, created_at::text AS created_at \
             FROM datasets WHERE id = $1",
        )
        .bind(id)
    } else {
        sqlx::query(
            "SELECT id, name, schema_json, primary_key, version, row_count, created_at::text AS created_at \
             FROM datasets WHERE name = $1",
        )
        .bind(selector)
    };
    query
        .fetch_optional(pool)
        .await
        .map_err(ApiError::database)?
        .ok_or_else(|| ApiError::not_found("DATASET_NOT_FOUND", "dataset not found"))
}

async fn select_metadata_for_update(
    transaction: &mut Transaction<'_, Postgres>,
    selector: &str,
) -> Result<sqlx::postgres::PgRow, ApiError> {
    let query = if let Ok(id) = Uuid::parse_str(selector) {
        sqlx::query(
            "SELECT id, name, schema_json, primary_key, version, row_count, created_at::text AS created_at \
             FROM datasets WHERE id = $1 FOR UPDATE",
        )
        .bind(id)
    } else {
        sqlx::query(
            "SELECT id, name, schema_json, primary_key, version, row_count, created_at::text AS created_at \
             FROM datasets WHERE name = $1 FOR UPDATE",
        )
        .bind(selector)
    };
    query
        .fetch_optional(&mut **transaction)
        .await
        .map_err(ApiError::database)?
        .ok_or_else(|| ApiError::not_found("DATASET_NOT_FOUND", "dataset not found"))
}

fn metadata_from_row(row: &sqlx::postgres::PgRow) -> Result<DatasetMetadata, ApiError> {
    let schema_json: SqlJson<Value> = row.try_get("schema_json").map_err(ApiError::internal)?;
    let schema = serde_json::from_value(schema_json.0)
        .map_err(|_| ApiError::internal("stored dataset schema is invalid"))?;
    Ok(DatasetMetadata {
        id: row.try_get("id").map_err(ApiError::internal)?,
        name: row.try_get("name").map_err(ApiError::internal)?,
        schema,
        primary_key: row.try_get("primary_key").map_err(ApiError::internal)?,
        version: row.try_get("version").map_err(ApiError::internal)?,
        row_count: row.try_get("row_count").map_err(ApiError::internal)?,
        created_at: row.try_get("created_at").map_err(ApiError::internal)?,
    })
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    code: &'static str,
    message: String,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code,
            message: message.into(),
        }
    }

    fn conflict(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code,
            message: message.into(),
        }
    }

    fn not_found(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code,
            message: message.into(),
        }
    }

    fn overloaded() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "OVERLOADED",
            message: "request concurrency limit reached; retry later".to_owned(),
        }
    }

    fn internal(error: impl std::fmt::Display) -> Self {
        tracing::error!(error = %error, "internal request error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL",
            message: "internal service error".to_owned(),
        }
    }

    fn database(error: sqlx::Error) -> Self {
        if error
            .as_database_error()
            .and_then(|database_error| database_error.code())
            .as_deref()
            == Some("23505")
        {
            return Self::conflict("ALREADY_EXISTS", "a dataset with that name already exists");
        }
        Self::internal(error)
    }

    fn model(error: ontology_core::ModelError) -> Self {
        Self::bad_request(error.code, error.message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code,
                message: self.message,
            },
        };
        let mut response = (self.status, Json(body)).into_response();
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_owned(),
                data_type: ApiDataType::Number,
            },
            ColumnDef {
                name: "label".to_owned(),
                data_type: ApiDataType::String,
            },
        ]
    }

    #[test]
    fn row_validation_uses_core_numeric_identity_and_types() {
        let values = vec![
            json!({"id": 1, "label": "one"}),
            json!({"id": 2.0, "label": "two"}),
        ];
        let rows = validate_rows("numbers", &schema(), "id", &values).expect("valid rows");
        assert_eq!(
            rows[0].key,
            canonical_primary_key_token("1", DataType::Number).unwrap()
        );
        assert_eq!(
            rows[1].key,
            canonical_primary_key_token("2.0", DataType::Number).unwrap()
        );
    }

    #[test]
    fn row_validation_rejects_duplicate_numeric_spellings() {
        let values = vec![
            json!({"id": 1, "label": "one"}),
            json!({"id": 1.0, "label": "duplicate"}),
        ];
        let error = validate_rows("numbers", &schema(), "id", &values).expect_err("duplicate key");
        assert_eq!(error.code, "OntologyMetadata:DuplicatePrimaryKey");
    }

    #[test]
    fn row_validation_rejects_wrong_typed_values() {
        let values = vec![json!({"id": "one", "label": 2})];
        assert_eq!(
            validate_rows("numbers", &schema(), "id", &values)
                .expect_err("wrong types")
                .code,
            "BAD_ROW_TYPE"
        );
    }

    #[test]
    fn request_limiter_enforces_the_configured_bound() {
        let limiter = RequestLimiter::new(2).expect("limiter");
        let first = limiter.try_enter().expect("first permit");
        let second = limiter.try_enter().expect("second permit");
        assert!(limiter.try_enter().is_err());
        drop(first);
        assert!(limiter.try_enter().is_ok());
        drop(second);
    }

    #[tokio::test]
    async fn request_limiter_is_safe_to_share_across_async_tasks() {
        let limiter = RequestLimiter::new(8).expect("limiter");
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let limiter = limiter.clone();
            tasks.push(tokio::spawn(async move {
                let Ok(_permit) = limiter.try_enter() else {
                    return false;
                };
                tokio::task::yield_now().await;
                true
            }));
        }
        let results = futures_free_join_all(tasks).await;
        assert!(results.iter().all(Result::is_ok));
    }

    async fn futures_free_join_all<T>(
        tasks: Vec<tokio::task::JoinHandle<T>>,
    ) -> Vec<Result<T, tokio::task::JoinError>> {
        let mut results = Vec::with_capacity(tasks.len());
        for task in tasks {
            results.push(task.await);
        }
        results
    }
}
