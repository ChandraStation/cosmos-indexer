use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomMsgSendToEth {
    pub sender: String,
    pub eth_dest: String,
    pub amount: Vec<CustomCoin>,
    pub bridge_fee: Vec<CustomCoin>,
    pub chain_fee: Vec<CustomCoin>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomMsgTransfer {
    pub source_port: String,
    pub source_channel: String,
    pub token: Vec<CustomCoin>,
    pub sender: String,
    pub receiver: String,
    pub timeout_height: Option<CustomHeight>,
    pub timeout_timestamp: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomHeight {
    pub revision_number: u64,
    pub revision_height: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CustomCoin {
    pub denom: String,
    pub amount: String,
}

#[derive(Serialize)]
pub struct ApiResponse {
    pub tx_hash: String,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomMsgSend {
    pub from_address: String,
    pub to_address: String,
    pub amount: Vec<CustomCoin>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CustomMsg {
    pub type_url: String,
    pub value: serde_json::Value,
}

#[derive(Serialize)]
pub struct TransactionResponse {
    pub tx_hash: String,
    pub block_number: u64,
    pub formatted_date: String,
    pub data: CustomMsgSend,
}
