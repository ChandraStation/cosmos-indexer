extern crate lazy_static;

pub mod transactions;
pub mod types;

const DEVELOPMENT: bool = cfg!(feature = "development");

const DOMAIN: &str = if cfg!(test) || DEVELOPMENT {
    "localhost"
} else {
    "localhost"
};
const PORT: u16 = 9000;

use actix_cors::Cors;
use actix_web::web::Path;
use actix_web::{
    error, get, middleware::Logger, middleware::NormalizePath, middleware::TrailingSlash, web, App,
    HttpResponse, HttpServer, Responder,
};
use shuttle_actix_web::ShuttleActixWeb;

use log::error;

use env_logger::Env;
use rocksdb::Options;
use rocksdb::DB;
use serde_json::json;

use clap::Parser;
use std::sync::Arc;
use transactions::database::transaction_info_thread;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(long, default_value = "http://66.172.36.142:2119")]
    chain_node_grpc: String,

    #[clap(long, default_value = "manifest")]
    chain_prefix: String,

    #[clap(long)]
    test_mode: bool,

    #[clap(long, default_value = "100000")]
    test_block_limit: u64,
}

#[get("/transactions/send")]
async fn get_all_msg_send_transactions(db: web::Data<Arc<DB>>) -> impl Responder {
    transactions::endpoints::get_all_msg_send_transactions(db).await
}

#[get("/transactions/ibc_transfer")]
async fn get_all_msg_ibc_transfer_transactions(db: web::Data<Arc<DB>>) -> impl Responder {
    transactions::endpoints::get_all_msg_ibc_transfer_transactions(db).await
}

#[get("/transactions/send/{address}")]
async fn get_msg_send_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: Path<String>,
) -> impl Responder {
    transactions::endpoints::get_msg_send_transactions_by_address(db, address.into_inner()).await
}

#[get("/transactions/send/{address}/{direction}")]
async fn get_msg_send_transactions_by_address_and_direction(
    db: web::Data<Arc<DB>>,
    path: Path<(String, String)>,
) -> impl Responder {
    let (address, direction) = path.into_inner();
    transactions::endpoints::get_msg_send_transactions_by_address_and_direction(
        db, address, direction,
    )
    .await
}

#[get("/transactions")]
async fn get_all_transactions(db: web::Data<Arc<DB>>) -> impl Responder {
    transactions::endpoints::get_all_transactions(db).await
}

#[get("/transactions/{address}")]
async fn get_all_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: Path<String>,
) -> impl Responder {
    transactions::endpoints::get_all_transactions_by_address(db, address.into_inner()).await
}

#[shuttle_runtime::main]
async fn actix_web(
    #[shuttle_shared_db::Postgres] pool: sqlx::PgPool,
) -> ShuttleActixWeb<impl FnOnce(&mut web::ServiceConfig) + Send + Clone + 'static> {
    // Initialize your RocksDB
    let mut db_options = rocksdb::Options::default();
    db_options.create_if_missing(true);
    let db = Arc::new(DB::open(&db_options, "transactions").expect("Failed to open database"));
    let api_db = web::Data::new(db.clone());

    // Initialize your transaction_info_thread
    // You might need to modify this to work with Shuttle's environment
    transactions::database::transaction_info_thread(
        db.clone(),
        std::env::var("CHAIN_NODE_GRPC")
            .unwrap_or_else(|_| "http://66.172.36.142:2119".to_string()),
        std::env::var("CHAIN_PREFIX").unwrap_or_else(|_| "manifest".to_string()),
        std::env::var("TEST_MODE").is_ok(),
        std::env::var("TEST_BLOCK_LIMIT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100000),
    );

    let config = move |cfg: &mut web::ServiceConfig| {
        cfg.app_data(api_db.clone())
            .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                error!("JSON error: {:?}", err);
                error::InternalError::from_response(err, HttpResponse::BadRequest().finish()).into()
            }))
            .wrap(Logger::default())
            .wrap(Logger::new("%a %{User-Agent}i"))
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allow_any_header()
                    .allow_any_method()
                    .max_age(3600),
            )
            .wrap(NormalizePath::new(TrailingSlash::Trim))
            .service(get_all_transactions)
            .service(get_all_transactions_by_address)
            .service(get_all_msg_send_transactions)
            .service(get_all_msg_ibc_transfer_transactions)
            .service(get_msg_send_transactions_by_address)
            .service(get_msg_send_transactions_by_address_and_direction)
            .service(web::scope("").default_service(web::route().to(|| async {
                HttpResponse::NotFound().json(json!({
                    "error": "Not Found",
                    "message": "The requested resource could not be found."
                }))
            })));
    };

    Ok(config.into())
}
