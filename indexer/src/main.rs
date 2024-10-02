extern crate lazy_static;
use crate::transactions::endpoints::PaginationParams;

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
use actix_web::web::{Path, Query};
use actix_web::{
    error, get, middleware::Logger, middleware::NormalizePath, middleware::TrailingSlash, web, App,
    HttpResponse, HttpServer, Responder,
};

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
async fn get_all_msg_send_transactions(db: web::Data<Arc<DB>>, pagination: Query<PaginationParams>) -> impl Responder {
    transactions::endpoints::get_all_msg_send_transactions(db, pagination).await
}

#[get("/transactions/ibc_transfer")]
async fn get_all_msg_ibc_transfer_transactions(db: web::Data<Arc<DB>>, pagination: Query<PaginationParams>) -> impl Responder {
    transactions::endpoints::get_all_msg_ibc_transfer_transactions(db, pagination).await
}

#[get("/transactions/send/{address}")]
async fn get_msg_send_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: Path<String>,
    pagination: Query<PaginationParams>,
) -> impl Responder {
    transactions::endpoints::get_msg_send_transactions_by_address(db, address.into_inner(), pagination).await
}

#[get("/transactions/send/{address}/{direction}")]
async fn get_msg_send_transactions_by_address_and_direction(
    db: web::Data<Arc<DB>>,
    path: Path<(String, String)>,
    pagination: Query<PaginationParams>,
) -> impl Responder {
    let (address, direction) = path.into_inner();
    transactions::endpoints::get_msg_send_transactions_by_address_and_direction(
        db, address, direction, pagination,
    )
    .await
}

#[get("/transactions")]
async fn get_all_transactions(db: web::Data<Arc<DB>>, pagination: Query<PaginationParams>,) -> impl Responder {
    transactions::endpoints::get_all_transactions(db, pagination).await
}

#[get("/transactions/{address}")]
async fn get_all_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: Path<String>,
    pagination: Query<PaginationParams>,
) -> impl Responder {
    transactions::endpoints::get_all_transactions_by_address(db, address.into_inner(), pagination).await
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    openssl_probe::init_ssl_cert_env_vars();
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let mut db_options = Options::default();
    db_options.create_if_missing(true);
    let db = Arc::new(DB::open(&db_options, "transactions").expect("Failed to open database"));
    let api_db = web::Data::new(db.clone());

    // Pass the arguments to the transaction_info_thread
    transaction_info_thread(
        db.clone(),
        args.chain_node_grpc,
        args.chain_prefix,
        args.test_mode,
        args.test_block_limit,
    );

    let server = HttpServer::new(move || {
        App::new()
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
            .app_data(api_db.clone())
            .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                error!("JSON error: {:?}", err);
                error::InternalError::from_response(err, HttpResponse::BadRequest().finish()).into()
            }))
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
            })))
    });

    let server = server.bind(format!("{}:{}", DOMAIN, PORT))?;
    server.run().await?;
    Ok(())
}
