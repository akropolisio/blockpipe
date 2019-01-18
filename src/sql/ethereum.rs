extern crate web3;
use sql::Sequelizable;
use std::fmt::LowerHex;
use web3::types::{Block, Log, Transaction, H160, H256};

impl Sequelizable for Transaction {
    fn table_name() -> &'static str {
        "transactions"
    }

    fn insert_fields() -> &'static str {
        "hash, nonce, blockHash, blockNumber, transactionIndex, \
         \"from\", \"to\", \"value\", gas, gasPrice"
    }

    fn to_insert_values(&self) -> String {
        format!(
            "DECODE('{:x}', 'hex'), {}, DECODE('{:x}', 'hex'), {}, {}, DECODE('{:x}', 'hex'), {}, {}, {}, {}",
                self.hash,
                self.nonce.as_u64(),
                self.block_hash.unwrap(),
                self.block_number.unwrap(),
                self.transaction_index.unwrap(),
                self.from,
                match self.to {
                    Some(dest) => format!("DECODE('{:x}', 'hex')", dest),
                    None => String::from("NULL")
                },
                self.value,
                self.gas,
                self.gas_price)
    }
}

impl<TX> Sequelizable for Block<TX> {
    fn table_name() -> &'static str {
        "blocks"
    }

    fn insert_fields() -> &'static str {
        "\"number\", hash, \"timestamp\""
    }

    fn to_insert_values(&self) -> String {
        format!(
            "{}, DECODE('{:x}', 'hex'), TO_TIMESTAMP({})",
            self.number.unwrap().as_u64(),
            self.hash.unwrap(),
            self.timestamp.as_u64()
        )
    }
}

#[inline]
fn to_decode<T: LowerHex>(inp: T) -> String {
    format!("DECODE('{:x}', 'hex')", inp)
}

impl Sequelizable for Log {
    fn table_name() -> &'static str {
        "logs"
    }

    fn insert_fields() -> &'static str {
        "address, data, block_hash, block_number,\
         transaction_hash, transaction_index, log_index, transaction_log_index,\
         log_type, removed"
    }

    fn to_insert_values(&self) -> String {
        format!(
            "DECODE('{:x}', 'hex'), DECODE('{:x?}', 'hex'), DECODE('{}', 'hex'), DECODE('{}', 'hex'),\
            DECODE('{}', 'hex'), DECODE('{}', 'hex'), DECODE('{}', 'hex'), DECODE('{}', 'hex'),\
            '{}'
            ",
            self.address,
            self.data.0,
            self.block_hash
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.block_number
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.transaction_hash
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.transaction_index
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.log_index
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.transaction_log_index
                .map(to_decode)
                .unwrap_or("NULL".to_string()),
            self.log_type.clone().map(|e| format!("'{}'", e)).unwrap_or("NULL".to_string())
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Topic {
    pub topic: H256,
    pub log_address: H160,
}

impl Sequelizable for Topic {
    fn table_name() -> &'static str {
        "topics"
    }

    fn insert_fields() -> &'static str {
        "topic, log_address"
    }

    fn to_insert_values(&self) -> String {
        format!(
            "DECODE('{:x}', 'hex'), DECODE('{:x}', 'hex')",
            self.topic, self.log_address,
        )
    }
}
