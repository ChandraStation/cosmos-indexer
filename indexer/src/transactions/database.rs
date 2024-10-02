use actix_rt::System;
use cosmos_sdk_proto_althea::{
    cosmos::bank::v1beta1::MsgSend,
    cosmos::tx::v1beta1::{TxBody, TxRaw},
    ibc::{applications::transfer::v1::MsgTransfer, core::client::v1::Height},
    tendermint::types::Block,
};
use deep_space::{client::Contact, utils::decode_any};
use futures::future::join_all;

use lazy_static::lazy_static;
use log::{error, info};
use rocksdb::DB;
use serde::Serialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{
    sync::{Arc, RwLock},
    thread,
    time::Instant,
};
use tokio::time::sleep;

use crate::types::{CustomCoin, CustomHeight, CustomMsgSend, CustomMsgTransfer};

pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

lazy_static! {
    static ref COUNTER: Arc<RwLock<Counters>> = Arc::new(RwLock::new(Counters {
        blocks: 0,
        transactions: 0,
        msgs: 0,
        ibc_msgs: 0,
        send_msgs: 0
    }));
}

pub struct Counters {
    blocks: u64,
    transactions: u64,
    msgs: u64,
    ibc_msgs: u64,
    send_msgs: u64, // Changed from send_eth_msgs
}
impl From<&Height> for CustomHeight {
    fn from(height: &Height) -> Self {
        CustomHeight {
            revision_number: height.revision_number,
            revision_height: height.revision_height,
        }
    }
}

impl From<&MsgSend> for CustomMsgSend {
    fn from(msg: &MsgSend) -> Self {
        CustomMsgSend {
            from_address: msg.from_address.clone(),
            to_address: msg.to_address.clone(),
            amount: msg
                .amount
                .iter()
                .map(|coin| CustomCoin {
                    denom: coin.denom.clone(),
                    amount: coin.amount.clone(),
                })
                .collect(),
        }
    }
}

impl From<&MsgTransfer> for CustomMsgTransfer {
    fn from(msg: &MsgTransfer) -> Self {
        CustomMsgTransfer {
            source_port: msg.source_port.clone(),
            source_channel: msg.source_channel.clone(),
            token: msg
                .token
                .as_ref()
                .map(|coin| CustomCoin {
                    denom: coin.denom.clone(),
                    amount: coin.amount.clone(),
                })
                .into_iter()
                .collect(),
            sender: msg.sender.clone(),
            receiver: msg.receiver.clone(),
            timeout_height: msg.timeout_height.as_ref().map(CustomHeight::from),
            timeout_timestamp: msg.timeout_timestamp,
        }
    }
}

const MAX_RETRIES: usize = 5;

/// finds earliest available block using binary search, keep in mind this cosmos
/// node will not have history from chain halt upgrades and could be state synced
/// and missing history before the state sync
/// Iterative implementation due to the limitations of async recursion in rust.
async fn get_earliest_block(contact: &Contact, mut start: u64, mut end: u64) -> u64 {
    while start <= end {
        let mid = start + (end - start) / 2;
        let mid_block = contact.get_block(mid).await;
        if let Ok(Some(_)) = mid_block {
            end = mid - 1;
        } else {
            start = mid + 1;
        }
    }
    // off by one error correction fix bounds logic up top
    start + 1
}

async fn get_latest_block(contact: &Contact) -> Result<u64, Box<dyn std::error::Error>> {
    let mut retries = 0;
    loop {
        match contact.get_chain_status().await {
            Ok(deep_space::client::ChainStatus::Moving { block_height }) => {
                return Ok(block_height);
            }
            Ok(_) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err("Failed to get moving chain status".into());
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    return Err(Box::new(e));
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

// Loads sendToEth & MsgTransfer messages from grpc endpoint & downlaods to DB
async fn search(contact: &Contact, start: u64, end: u64, db: &DB) {
    if start > end {
        return;
    }
    let mut current_start = start;
    let retries = AtomicUsize::new(0);

    loop {
        let blocks_result = contact.get_block_range(current_start, end).await;

        let blocks = match blocks_result {
            Ok(result) => {
                retries.store(0, Ordering::Relaxed);
                result
            }
            Err(e) => {
                let current_retries = retries.fetch_add(1, Ordering::Relaxed);
                if current_retries >= MAX_RETRIES {
                    error!("Error getting block range: {:?}, exceeded max retries", e);
                    break;
                } else {
                    error!("Error getting block range: {:?}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        };

        if blocks.is_empty() {
            break;
        }

        // gets the last block that was successfully fetched to be referenced
        // in case of grpc error
        let last_block_height = blocks
            .last()
            .unwrap()
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
            .height;

        // counters for transactions, messages, blocks & tx types
        let mut tx_counter = 0;
        let mut msg_counter = 0;
        let mut ibc_transfer_counter = 0;
        let mut send_msg_counter = 0;
        let blocks_len = blocks.len() as u64;

        for block in blocks.into_iter() {
            let block = block.unwrap();
            // Get the block number
            let block_number = block.header.as_ref().unwrap().height;

            // tx fetching
            for tx in block.data.unwrap().txs {
                let raw_tx_any = prost_types::Any {
                    type_url: "/cosmos.tx.v1beta1.TxRaw".to_string(),
                    value: tx,
                };
                let tx_raw: TxRaw = decode_any(raw_tx_any.clone()).unwrap();
                let value_ref: &[u8] = raw_tx_any.value.as_ref();
                let tx_hash = sha256::digest(value_ref).to_uppercase();
                let body_any = prost_types::Any {
                    type_url: "/cosmos.tx.v1beta1.TxBody".to_string(),
                    value: tx_raw.body_bytes,
                };
                let tx_body: TxBody = decode_any(body_any).unwrap();

                let mut has_msg_ibc_transfer = false;

                // tx sorting
                for message in tx_body.messages {
                    if message.type_url == "/cosmos.bank.v1beta1.MsgSend" {
                        msg_counter += 1;

                        let msg_send_any = prost_types::Any {
                            type_url: "/cosmos.bank.v1beta1.MsgSend".to_string(),
                            value: message.value,
                        };
                        let msg_send: Result<MsgSend, _> = decode_any(msg_send_any);

                        if let Ok(msg_send) = msg_send {
                            let custom_msg_send = CustomMsgSend::from(&msg_send);
                            let timestamp = block
                                .header
                                .as_ref()
                                .unwrap()
                                .time
                                .as_ref()
                                .unwrap()
                                .seconds;
                            let key =
                                format!("{:012}:msgSend:{}:{}", block_number, timestamp, tx_hash);
                            save_msg(db, &key, &custom_msg_send);
                            send_msg_counter += 1;
                        }
                    } else if message.type_url == "/ibc.applications.transfer.v1.MsgTransfer" {
                        has_msg_ibc_transfer = true;
                        msg_counter += 1;

                        let msg_ibc_transfer_any = prost_types::Any {
                            type_url: "/ibc.applications.transfer.v1.MsgTransfer".to_string(),
                            value: message.value,
                        };
                        let msg_ibc_transfer: Result<MsgTransfer, _> =
                            decode_any(msg_ibc_transfer_any);

                        if let Ok(msg_ibc_transfer) = msg_ibc_transfer {
                            let custom_ibc_transfer = CustomMsgTransfer::from(&msg_ibc_transfer);
                            let timestamp = block
                                .header
                                .as_ref()
                                .unwrap()
                                .time
                                .as_ref()
                                .unwrap()
                                .seconds;
                            let key = format!(
                                "{:012}:msgIbcTransfer:{}:{}",
                                block_number, timestamp, tx_hash
                            );
                            save_msg(db, &key, &custom_ibc_transfer);
                        }
                    }
                }

                if has_msg_ibc_transfer {
                    tx_counter += 1;
                    ibc_transfer_counter += 1;
                }
            }
            current_start = (last_block_height as u64) + 1;
            if current_start > end {
                break;
            }
        }
        let mut c = COUNTER.write().unwrap();
        c.blocks += blocks_len;
        c.transactions += tx_counter;
        c.msgs += msg_counter;
        c.ibc_msgs += ibc_transfer_counter;
        c.send_msgs += send_msg_counter;
    }
}

async fn continuous_indexing(db: &DB, chain_node_grpc: &str, chain_prefix: &str) {
    let contact: Contact = Contact::new(chain_node_grpc, REQUEST_TIMEOUT, chain_prefix).unwrap();

    loop {
        let last_indexed_block = load_last_download_block(db).unwrap_or(0);
        let latest_block = match get_latest_block(&contact).await {
            Ok(block) => block,
            Err(e) => {
                error!("Error getting latest block: {:?}", e);
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        if latest_block > last_indexed_block {
            for block_height in (last_indexed_block + 1)..=latest_block {
                match contact.get_block(block_height).await {
                    Ok(Some(block)) => {
                        process_block(&contact, &block, db).await;
                        info!("Processed block {}", block_height);
                    }
                    Ok(None) => {
                        error!("Block {} not found", block_height);
                    }
                    Err(e) => {
                        error!("Error fetching block {}: {:?}", block_height, e);
                    }
                }
            }
            save_last_download_block(db, latest_block);
        }

        sleep(Duration::from_secs(5)).await;
    }
}

async fn process_block(_contact: &Contact, block: &Block, db: &DB) {
    let block_number = block.header.as_ref().unwrap().height;
    let timestamp = block
        .header
        .as_ref()
        .unwrap()
        .time
        .as_ref()
        .unwrap()
        .seconds;

    for tx in block.data.as_ref().unwrap().txs.iter() {
        let raw_tx_any = prost_types::Any {
            type_url: "/cosmos.tx.v1beta1.TxRaw".to_string(),
            value: tx.clone(),
        };
        let tx_raw: TxRaw = decode_any(raw_tx_any.clone()).unwrap();
        let value_ref: &[u8] = raw_tx_any.value.as_ref();
        let tx_hash = sha256::digest(value_ref).to_uppercase();
        let body_any = prost_types::Any {
            type_url: "/cosmos.tx.v1beta1.TxBody".to_string(),
            value: tx_raw.body_bytes,
        };
        let tx_body: TxBody = decode_any(body_any).unwrap();

        for message in tx_body.messages {
            info!("Processing message: {:?}", message.type_url);
            match message.type_url.as_str() {
                "/cosmos.bank.v1beta1.MsgSend" => {
                    let msg_send: MsgSend = decode_any(message).unwrap();
                    let custom_msg_send = CustomMsgSend::from(&msg_send);
                    let key = format!("{:012}:msgSend:{}:{}", block_number, timestamp, tx_hash);
                    save_msg(db, &key, &custom_msg_send);
                }
                "/ibc.applications.transfer.v1.MsgTransfer" => {
                    let msg_ibc_transfer: MsgTransfer = decode_any(message).unwrap();
                    let custom_ibc_transfer = CustomMsgTransfer::from(&msg_ibc_transfer);
                    let key = format!(
                        "{:012}:msgIbcTransfer:{}:{}",
                        block_number, timestamp, tx_hash
                    );
                    save_msg(db, &key, &custom_ibc_transfer);
                }
                _ => {}
            }
        }
    }
}

pub fn transaction_info_thread(
    db: Arc<DB>,
    chain_node_grpc: String,
    chain_prefix: String,
    test_mode: bool,
    test_block_limit: u64,
) {
    info!("Starting transaction info thread");

    thread::spawn(move || {
        let runner = System::new();
        runner.block_on(async {
            loop {
                match transactions(
                    &db,
                    &chain_node_grpc,
                    &chain_prefix,
                    test_mode,
                    test_block_limit,
                )
                .await
                {
                    Ok(_) => {
                        continuous_indexing(&db, &chain_node_grpc, &chain_prefix).await;
                    }
                    Err(e) => {
                        error!("Error downloading transactions: {:?}", e);
                        let mut retry_interval = Duration::from_secs(1);
                        loop {
                            info!("Retrying block download");
                            sleep(retry_interval).await;
                            match transactions(
                                &db,
                                &chain_node_grpc,
                                &chain_prefix,
                                test_mode,
                                test_block_limit,
                            )
                            .await
                            {
                                Ok(_) => break,
                                Err(e) => {
                                    error!("Error in transaction download retry: {:?}", e);
                                    retry_interval =
                                        if let Some(new_interval) = retry_interval.checked_mul(2) {
                                            new_interval
                                        } else {
                                            retry_interval
                                        };
                                }
                            }
                        }
                    }
                }
            }
        });
    });
}

/// creates batches of transactions found and sorted using the search function
/// then writes them to the db
pub async fn transactions(
    db: &DB,
    chain_node_grpc: &str,
    chain_prefix: &str,
    test_mode: bool,
    test_block_limit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Started downloading & parsing transactions");
    let contact: Contact = Contact::new(chain_node_grpc, REQUEST_TIMEOUT, chain_prefix)?;

    let mut retries = 0;
    let status = loop {
        let result = contact.get_chain_status().await;

        match result {
            Ok(chain_status) => {
                break chain_status;
            }
            Err(e) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    error!("Failed to get chain status, grpc error: {:?}", e);
                    return Err(Box::new(e));
                } else {
                    error!("Failed to get chain status, grpc error: {:?}, retrying", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    };

    // get the latest block this node has
    let mut current_status = status;
    let latest_block;
    loop {
        match current_status {
            deep_space::client::ChainStatus::Moving { block_height } => {
                latest_block = Some(block_height);
                break;
            }
            _ => match contact.get_chain_status().await {
                Ok(chain_status) => {
                    if let deep_space::client::ChainStatus::Moving { block_height } = chain_status {
                        latest_block = Some(block_height);
                        break;
                    }
                    current_status = chain_status;
                }
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        error!("Failed to get chain status: {:?}", e);
                        return Err(Box::new(e));
                    } else {
                        error!("Failed to get chain status: {:?}, retrying", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            },
        }
    }

    let latest_block = latest_block.expect("Node is not synced or not running");

    // now we find the earliest block this node has via binary search, we could just read it from
    // the error message you get when requesting an earlier block, but this was more fun
    let earliest_block = get_earliest_block(&contact, 0, latest_block).await;

    let earliest_block = match load_last_download_block(db) {
        Some(block) => block,
        None => earliest_block,
    };

    let end_block = if test_mode {
        std::cmp::min(earliest_block + test_block_limit, latest_block)
    } else {
        latest_block
    };

    info!("There are already {} blocks in the database.", latest_block,);

    info!(
        "This node has {} blocks to download, downloading to database",
        latest_block - earliest_block
    );
    let start = Instant::now();

    // how many blocks to search per future
    const BATCH_SIZE: u64 = 500;
    // how many futures to execute at once
    const EXECUTE_SIZE: usize = 10;
    let mut pos = earliest_block;
    let mut futures = Vec::new();
    while pos < end_block {
        let start = pos;
        let end = if end_block - pos > BATCH_SIZE {
            pos += BATCH_SIZE;
            pos
        } else {
            pos = end_block;
            end_block
        };
        let fut = search(&contact, start, end, db);
        futures.push(fut);
    }

    let futures = futures.into_iter();

    let mut buf = Vec::new();

    for fut in futures {
        if buf.len() < EXECUTE_SIZE {
            buf.push(fut);
        } else {
            let _ = join_all(buf).await;
            info!(
                "Completed batch of {} blocks",
                BATCH_SIZE * EXECUTE_SIZE as u64
            );
            buf = Vec::new();
        }
    }
    let _ = join_all(buf).await;

    let counter = COUNTER.read().unwrap();
    info!(
    "Successfully downloaded {} blocks and {} tx containing {} send msgs and {} ibc_transfer msgs in {} seconds",
    counter.blocks,
    counter.transactions,
    counter.send_msgs,
    counter.ibc_msgs,
    start.elapsed().as_secs()
);
    save_last_download_block(db, end_block);
    Ok(())
}

pub fn save_msg<T: Serialize>(db: &DB, key: &str, data: &T) {
    let data_json = serde_json::to_string(data).unwrap();
    db.put(key.as_bytes(), data_json.as_bytes()).unwrap();
}

// timestamp function using downloaded blocks as a source of truth
const LAST_DOWNLOAD_BLOCK_KEY: &str = "last_download_block";

fn save_last_download_block(db: &DB, timestamp: u64) {
    db.put(
        LAST_DOWNLOAD_BLOCK_KEY.as_bytes(),
        timestamp.to_string().as_bytes(),
    )
    .unwrap();
}

fn load_last_download_block(db: &DB) -> Option<u64> {
    let res = db.get(LAST_DOWNLOAD_BLOCK_KEY.as_bytes()).unwrap();
    res.map(|bytes| String::from_utf8_lossy(&bytes).parse::<u64>().unwrap())
}
