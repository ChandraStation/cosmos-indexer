use crate::types::{ApiResponse, CustomMsgSend, CustomMsgTransfer};

use actix_web::Responder;
use actix_web::{web, HttpResponse};
use chrono::{DateTime, Datelike, Local, NaiveDateTime, Utc};

use log::error;

use rocksdb::DB;

use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use serde_derive::Deserialize;

const DEFAULT_PAGE_SIZE: usize = 10;

#[derive(Serialize)]
struct BlockTransactions {
    block_number: u64,
    transactions: Vec<ApiResponse>,
    formatted_date: String,
}

#[derive(Serialize)]
struct TransactionResponse {
    tx_hash: String,
    block_number: u64,
    formatted_date: String,
    data: CustomMsgSend,
}

#[derive(Serialize)]
struct AllTransactionResponse {
    tx_hash: String,
    block_number: u64,
    formatted_date: String,
    transaction_type: String,
    data: serde_json::Value,
}

#[derive(Deserialize)]
pub struct PaginationParams {
    page: Option<usize>,
    page_size: Option<usize>,
}

type BlockData = (String, Vec<ApiResponse>);

pub async fn get_all_transactions(db: web::Data<Arc<DB>>,  web::Query(pagination): web::Query<PaginationParams>,) -> impl Responder {
    let transactions = get_all_filtered_transactions(&db, None, pagination.page, pagination.page_size);
    HttpResponse::Ok().json(transactions)
}

pub async fn get_all_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: String,
    web::Query(pagination): web::Query<PaginationParams>,
) -> impl Responder {
    let transactions = get_all_filtered_transactions(&db, Some(&address), pagination.page, pagination.page_size);
    HttpResponse::Ok().json(transactions)
}

fn get_all_filtered_transactions(
    db: &Arc<DB>,
    address: Option<&str>,
    page: Option<usize>,
    page_size: Option<usize>,
) -> Vec<AllTransactionResponse> {
    let mut response_data = Vec::new();
    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        if let Ok((key, value)) = item {
            let key_str = String::from_utf8_lossy(&key);
            let key_parts: Vec<&str> = key_str.split(':').collect();
            if key_parts.len() == 4 {
                let transaction_type = key_parts[1];
                let block_number = key_parts[0].parse::<u64>().unwrap();
                let timestamp = key_parts[2].parse::<i64>().unwrap();
                let formatted_date = format_date(timestamp);

                let (data, is_relevant) = match transaction_type {
                    "msgSend" => {
                        let msg_send: CustomMsgSend = serde_json::from_slice(&value).unwrap();
                        let is_relevant = address.map_or(true, |addr| {
                            msg_send.from_address == addr || msg_send.to_address == addr
                        });
                        (serde_json::to_value(&msg_send).unwrap(), is_relevant)
                    }
                    "msgIbcTransfer" => {
                        let msg_transfer: CustomMsgTransfer =
                            serde_json::from_slice(&value).unwrap();
                        let is_relevant = address.map_or(true, |addr| {
                            msg_transfer.sender == addr || msg_transfer.receiver == addr
                        });
                        (serde_json::to_value(&msg_transfer).unwrap(), is_relevant)
                    }
                    _ => continue,
                };

                if is_relevant {
                    response_data.push(AllTransactionResponse {
                        tx_hash: key_parts[3].to_string(),
                        block_number,
                        formatted_date,
                        transaction_type: transaction_type.to_string(),
                        data,
                    });
                }
            }
        }
    }

    response_data.sort_by(|a, b| b.block_number.cmp(&a.block_number));

    let page = page.unwrap_or(1);
    let page_size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let start = (page - 1) * page_size;

    response_data.into_iter().skip(start).take(page_size).collect()
}

pub async fn get_msg_send_transactions_by_address(
    db: web::Data<Arc<DB>>,
    address: String,
    web::Query(pagination): web::Query<PaginationParams>,
) -> impl Responder {
    let transactions = get_filtered_transactions(&db, &address, None, pagination.page, pagination.page_size);
    HttpResponse::Ok().json(transactions)
}

pub async fn get_msg_send_transactions_by_address_and_direction(
    db: web::Data<Arc<DB>>,
    address: String,
    direction: String,
    web::Query(pagination): web::Query<PaginationParams>,
) -> impl Responder {
    let direction = match direction.as_str() {
        "send" => Some(true),
        "receive" => Some(false),
        _ => return HttpResponse::BadRequest().body("Invalid direction. Use 'send' or 'receive'."),
    };

    let transactions = get_filtered_transactions(&db, &address, direction, pagination.page, pagination.page_size);
    HttpResponse::Ok().json(transactions)
}

fn get_filtered_transactions(
    db: &Arc<DB>,
    address: &str,
    is_sender: Option<bool>,
    page: Option<usize>,
    page_size: Option<usize>,
) -> Vec<TransactionResponse> {
    let mut response_data = Vec::new();
    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        if let Ok((key, value)) = item {
            let key_str = String::from_utf8_lossy(&key);
            let key_parts: Vec<&str> = key_str.split(':').collect();
            if key_parts.len() == 4 && key_parts[1] == "msgSend" {
                let msg_send: CustomMsgSend = serde_json::from_slice(&value).unwrap();

                let is_sender_match = match is_sender {
                    Some(true) => msg_send.from_address == address,
                    Some(false) => msg_send.to_address == address,
                    None => msg_send.from_address == address || msg_send.to_address == address,
                };

                if is_sender_match {
                    let block_number = key_parts[0].parse::<u64>().unwrap();
                    let timestamp = key_parts[2].parse::<i64>().unwrap();
                    let formatted_date = format_date(timestamp);

                    response_data.push(TransactionResponse {
                        tx_hash: key_parts[3].to_string(),
                        block_number,
                        formatted_date,
                        data: msg_send,
                    });
                }
            }
        }
    }

    response_data.sort_by(|a, b| b.block_number.cmp(&a.block_number));

    let page = page.unwrap_or(1);
    let page_size = page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let start = (page - 1) * page_size;

    response_data.into_iter().skip(start).take(page_size).collect()
}

fn format_date(timestamp: i64) -> String {
    let naive = chrono::NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap();
    let datetime: chrono::DateTime<chrono::Utc> = chrono::DateTime::from_utc(naive, chrono::Utc);
    let datetime_local: chrono::DateTime<chrono::Local> = datetime.into();
    datetime_local.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub async fn get_all_msg_send_transactions(db: web::Data<Arc<DB>>, web::Query(pagination): web::Query<PaginationParams>) -> impl Responder {
    let mut response_data = Vec::new();
    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        match item {
            Ok((key, value)) => {
                let key_str = String::from_utf8_lossy(&key);
                let key_parts: Vec<&str> = key_str.split(':').collect();
                if key_parts.len() == 4 && key_parts[1] == "msgSend" {
                    match serde_json::from_slice::<CustomMsgSend>(&value) {
                        Ok(msg_send) => {
                            let block_number = key_parts[0].parse::<u64>().unwrap();
                            let timestamp = key_parts[2].parse::<i64>().unwrap();

                            let formatted_date = format_date(timestamp);

                            response_data.push(TransactionResponse {
                                tx_hash: key_parts[3].to_string(),
                                block_number,
                                formatted_date,
                                data: msg_send,
                            });
                        }
                        Err(e) => error!("Failed to deserialize msgSend: {}", e),
                    }
                }
            }
            Err(err) => {
                error!("RocksDB iterator error: {}", err);
            }
        }
    }

    response_data.sort_by(|a, b| b.block_number.cmp(&a.block_number));

    let page = pagination.page.unwrap_or(1);
    let page_size = pagination.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let start = (page - 1) * page_size;

    HttpResponse::Ok().json(response_data.into_iter().skip(start).take(page_size).collect::<Vec<_>>())
}

pub async fn get_all_msg_ibc_transfer_transactions(db: web::Data<Arc<DB>>, web::Query(pagination): web::Query<PaginationParams>) -> impl Responder {
    let mut response_data: HashMap<u64, BlockData> = HashMap::new();

    let iterator = db.iterator(rocksdb::IteratorMode::Start);

    for item in iterator {
        match item {
            Ok((key, value)) => {
                let key_str = String::from_utf8_lossy(&key);
                let key_parts: Vec<&str> = key_str.split(':').collect();
                if key_parts.len() == 4 && key_parts[1] == "msgIbcTransfer" {
                    let msg_ibc_transfer: CustomMsgTransfer =
                        serde_json::from_slice(&value).unwrap();
                    let block_number = key_parts[0].parse::<u64>().unwrap();

                    let timestamp = key_parts[2].parse::<i64>().unwrap();

                    // Convert timestamp to Option<NaiveDateTime>
                    let naive_opt = NaiveDateTime::from_timestamp_opt(timestamp, 0);

                    let mut _datetime_utc: Option<DateTime<Utc>> = None;

                    if let Some(naive_datetime) = naive_opt {
                        // Convert Option<NaiveDateTime> to DateTime
                        _datetime_utc = Some(DateTime::<Utc>::from_utc(naive_datetime, Utc));
                    } else {
                        error!("Invalid timestamp: {}", timestamp);
                        continue; // skip this iteration if timestamp is invalid
                    }

                    let datetime_utc = _datetime_utc.unwrap(); // we can safely unwrap because of the `continue` above

                    let datetime_local: DateTime<Local> = datetime_utc.into();

                    // Extract month, day, and year
                    let month = datetime_local.month();
                    let day = datetime_local.day();
                    let year = datetime_local.year();

                    // Format the date string
                    let formatted_date = format!("{:02}-{:02}-{}", month, day, year);
                    let api_response = ApiResponse {
                        tx_hash: key_parts[3].to_string(),
                        data: serde_json::to_value(&msg_ibc_transfer).unwrap(),
                    };

                    response_data
                        .entry(block_number)
                        .or_insert((formatted_date, Vec::new()))
                        .1
                        .push(api_response);
                }
            }
            Err(err) => {
                error!("RocksDB iterator error: {}", err);
            }
        }
    }

    // Converting the HashMap to a Vec and sorting it by block number
    let mut response_data: Vec<_> = response_data.into_iter().collect();
    response_data.sort_by(|a, b| a.0.cmp(&b.0));

    // Convert Vec of tuples into Vec of BlockTransactions
    let response_data: Vec<_> = response_data
        .into_iter()
        .map(
            |(block_number, (formatted_date, transactions))| BlockTransactions {
                block_number,
                formatted_date,
                transactions,
            },
        )
        .collect();

    let page = pagination.page.unwrap_or(1);
    let page_size = pagination.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let start = (page - 1) * page_size;

    HttpResponse::Ok().json(response_data.into_iter().skip(start).take(page_size).collect::<Vec<_>>())
}
