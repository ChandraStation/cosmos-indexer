# Cosmos Indexer

### Summary/Credits

An API, built in Rust, that indexes and stores Cosmos messages with rocksDB. This is a continuation/fork of work that was done for [Gravity Bridge]()'s [gravity-info-api]() by Chandra Station to store and index transactions in conjunction with the analytical functionality and core API framework built by [Justin Kilpatrick]() at [Althea]() & [Gravity Bridge](),

Using GRPC, a connection is made to a node with bocks. The blocks are then filtered for messages, then message types, and finally stored to be served via the web api. The api provides various endpoints to deliver the transaction data in a structured format.

## Installation/Dependencies

1. Clone the repository

   ```
   git clone https://github.com/chandrastation/cosmos-indexer
   cd cosmos-indexer/indexer
   ```

2. Install Rust

   Linux:

   ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

   OR
   MacOS:

   ```bash
   brew install rust
   ```

## Dev Environment

In the development environment you can set variables in the command when you run the server or just run the command without variables and the server will run with the default values.

### Variable List

- `CHAIN_NODE_GRPC`: The GRPC endpoint of the node you want to connect to.
- `CHAIN_PREFIX`: The prefix of the chain you are connecting to.
- `TEST_MODE`: If you want to run the server in test mode.
- `TEST_BLOCK_LIMIT`: The block limit for the test mode.

### Run the server

```
sudo cargo run --features development
```

OR

```
sudo cargo run --features development -- --chain-node-grpc <endpoint> --chain-prefix <prefix> --test-mode <true/false> --test-block-limit <block_limit>
```

## API Docs

### /transactions

Provides all blocks that contain any transactions on the provided chain and the data of the transactions.

- URL: `http://localhost:9000/transactions`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions`

---

### /transactions/{address}

Provides all blocks that contain any transactions for the specified address on the provided chain and the data of those transactions.

- URL: `http://localhost:9000/transactions/{address}`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions/manifest1uwqjtgjhjctjc45ugy7ev5prprhehc7wclherd`

---

### /transactions/send

Provides all blocks that contain MsgSend transactions on the provided chain and the data of the transactions.

- URL: `http://localhost:9000/transactions/send`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions/send`

---

### /transactions/send/{address}

Provides all blocks that contain MsgSend transactions on the provided chain and the data of the transactions for the specific wallet.

- URL: `http://localhost:9000/transactions/send/{address}/`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions/send/manifest1uwqjtgjhjctjc45ugy7ev5prprhehc7wclherd`

---

### /transactions/send/{address}/send

Provides all blocks that contain MsgSend transactions on the provided chain and the data of the transactions for the specific wallet where the address was the sender.

- URL: `http://localhost:9000/transactions/send/{address}/send`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions/send/manifest1uwqjtgjhjctjc45ugy7ev5prprhehc7wclherd/send`

---

### /transactions/send/{address}/receive

Provides all blocks that contain MsgSend transactions on the provided chain and the data of the transactions for the specific wallet where the address was the receiver.

- URL: `http://localhost:9000/transactions/send/{address}/receive`
- Method: `GET`
- URL Params: `None`
- Data Params: `None`
- Success Response:
  - Code: 200 OK
  - Contents:

```json
{
  "block_number": 1850,
  "transactions": [
    {
      "tx_hash": "6BEB689E0589C01663A460B20363712B8049A12AE1094CDFE543C5973A8F26C4",
      "data": {
        "amount": [
          {
            "amount": "4000000",
            "denom": "umfx"
          }
        ],
        "from_address": "manifest1wxjfftrc0emj5f7ldcvtpj05lxtz3t2npghwsf",
        "to_address": "manifest1afk9zr2hn2jsac63h4hm60vl9z3e5u69gndzf7c99cqge3vzwjzsfmy9qj"
      }
    }
  ],
  "formatted_date": "04-10-2024"
}
```

- Error Response: `500 Server Error`

- Sample Call:

`curl http://localhost:9000/transactions/send/manifest1uwqjtgjhjctjc45ugy7ev5prprhehc7wclherd/receive`

---
