use postgres::{Connection, TlsMode};
use std::{thread, time};

use std;
use std::fmt::Write;
use std::string::String;

use sql;
use sql::{Sequelizable, Topic};
use web3;
use web3::futures::Future;
use web3::transports::EventLoopHandle;
use web3::types::{
    Block, BlockId, BlockNumber, FilterBuilder, Log, SyncState, Transaction,
};
use web3::Transport;
use web3::Web3;

mod error;

const MAX_BLOCKS_PER_BATCH: i32 = 100;

#[allow(dead_code)]
pub struct Pipe<T: Transport> {
    eloop: EventLoopHandle, // needs to be held for event loop to be owned right
    web3: Web3<T>,
    pg_client: Connection,
    last_db_block: u64, // due to BIGINT and lack of NUMERIC support in driver
    last_node_block: u64,
    syncing: bool,
}

impl<T: Transport> Pipe<T> {
    const ONE_MINUTE: time::Duration = time::Duration::from_secs(60);

    pub fn new(
        transport: T,
        eloop: EventLoopHandle,
        pg_path: &str,
    ) -> Result<Pipe<T>, Box<std::error::Error>> {
        let pg_client = Connection::connect(pg_path, TlsMode::None)?;

        let rows = pg_client.query(sql::LAST_DB_BLOCK_QUERY, &[])?;
        let last_db_block_number: i64 = match rows.iter().next() {
            Some(row) => row.get(0),
            None => 0,
        };

        let web3 = Web3::new(transport);

        Ok(Pipe {
            eloop: eloop,
            web3: web3,
            pg_client: pg_client,
            last_db_block: last_db_block_number as u64,
            last_node_block: 0,
            syncing: false,
        })
    }

    fn update_node_info(&mut self) -> Result<bool, web3::Error> {
        println!("Getting info from eth node.");
        let last_block_number = self.web3.eth().block_number().wait()?.as_u64();
        let syncing = self.web3.eth().syncing().wait()?;

        println!(">> last_block_number: {}", last_block_number);

        self.last_node_block = last_block_number;
        self.syncing = syncing != SyncState::NotSyncing;
        Ok(true)
    }

    fn sleep_with_msg(msg: &str) {
        println!("{}", msg);
        thread::sleep(Self::ONE_MINUTE);
    }

    fn sleep_when_syncing(&self) -> bool {
        if self.syncing {
            Self::sleep_with_msg("Node is syncing, sleeping for a minute.");
            return true;
        }

        false
    }

    fn write_insert_header<S: Sequelizable>(
        mut sql_query: &mut String,
    ) -> Result<(), std::fmt::Error> {
        write!(
            &mut sql_query,
            "INSERT INTO {}({}) VALUES\n",
            S::table_name(),
            S::insert_fields()
        )
    }

    fn trim_ends(sql_query: &mut String) {
        sql_query.pop(); // remove \n
        sql_query.pop(); // remove ,
    }

    fn store_next_batch(&mut self) -> Result<i32, error::PipeError> {
        let mut next_block_number = self.last_db_block + 1;

        let mut processed: i32 = 0;
        let mut processed_tx: i32 = 0;
        let mut processed_logs: i32 = 0;
        let mut processed_topics: i32 = 0;

        let mut insert_blocks: SqlInsert<Block<Transaction>> =
            SqlInsert::new(1096);
        let mut insert_transactions: SqlInsert<Transaction> =
            SqlInsert::new(8096);

        let mut sql_logs: String = String::with_capacity(8096 * 1024 * 10);
        let mut sql_topics: String = String::with_capacity(1096 * 1024 * 10);

        insert_blocks.start()?;
        insert_transactions.start()?;

        Self::write_insert_header::<Log>(&mut sql_logs)?;
        Self::write_insert_header::<Topic>(&mut sql_topics)?;

        while processed < MAX_BLOCKS_PER_BATCH
            && next_block_number <= self.last_node_block
        {
            let block = self
                .web3
                .eth()
                .block_with_txs(BlockId::from(next_block_number))
                .wait()?
                .unwrap();
            next_block_number += 1;
            processed += 1;

            insert_blocks.insert(block.clone());

            for tx in block.transactions.iter() {
                insert_transactions.insert(tx.clone());
            }

            if let Some(block_number) = block.number {
                let logs = self
                    .web3
                    .eth()
                    .logs(
                        FilterBuilder::default()
                            .from_block(BlockNumber::from(
                                block_number.low_u64(),
                            ))
                            .build(),
                    )
                    .wait()?;

                for log in logs {
                    write!(&mut sql_logs, "({}),\n", log.to_insert_values())?;
                    processed_logs += 1;

                    for topic in log.topics {
                        write!(
                            &mut sql_topics,
                            "({}),\n",
                            sql::Topic {
                                topic,
                                log_address: log.address
                            }
                            .to_insert_values()
                        )?;
                        processed_topics += 1;
                    }
                }
            }
        }

        if processed == 0 {
            return Ok(0);
        }

        Self::trim_ends(&mut sql_logs);
        Self::trim_ends(&mut sql_topics);

        let pg_tx = self.pg_client.transaction()?;

        // save the blocks
        insert_blocks.execute(&pg_tx)?;

        // upsert in case of reorg
        insert_transactions.execute_with(&pg_tx, "\nON CONFLICT (hash) DO UPDATE SET nonce = excluded.nonce, blockHash = excluded.blockHash, blockNumber = excluded.blockNumber, transactionIndex = excluded.transactionIndex, \"from\" = excluded.from, \"to\" = excluded.to, \"value\" = excluded.value, gas = excluded.gas, gasPrice = excluded.gasPrice".to_string())?;

        if processed_logs > 0 {
            // save logs
            println!("{}", sql_logs);
            pg_tx.execute(&sql_logs, &[])?;
        }

        if processed_topics > 0 {
            println!("{}", sql_topics);
            pg_tx.execute(&sql_topics, &[])?;
        }

        pg_tx.commit()?;

        self.last_db_block = next_block_number - 1;
        println!(
            "Processed {} blocks. At {}/{}",
            processed, self.last_db_block, self.last_node_block
        );
        Ok(processed)
    }

    pub fn run(&mut self) -> Result<i32, error::PipeError> {
        loop {
            self.update_node_info()?;
            if self.sleep_when_syncing() {
                println!(">> sleep_when_syncing == true");
                continue;
            }

            println!(
                "Queue size: {}",
                self.last_node_block - self.last_db_block
            );

            println!(
                "last_db_block: {}, last_node_block: {}",
                self.last_db_block, self.last_node_block
            );

            while self.last_db_block < self.last_node_block {
                self.store_next_batch()?;
            }

            Self::sleep_with_msg("Run done, sleeping for one minute.")
        }
    }
}

struct SqlInsert<T> {
    processed: u32,
    sql: String,
    items: Vec<T>,
}

impl<T: Sequelizable> SqlInsert<T> {
    fn new(size: usize) -> Self {
        SqlInsert {
            processed: 0,
            sql: String::with_capacity(size * 1024 * 10),
            items: Vec::new(),
        }
    }

    fn start(&mut self) -> Result<(), std::fmt::Error> {
        write!(
            &mut self.sql,
            "INSERT INTO {} ({}) VALUES\n",
            T::table_name(),
            T::insert_fields()
        )
    }

    fn insert(&mut self, value: T) {
        self.items.push(value);
        self.processed += 1;
    }

    fn get_sql(&self) -> Option<String> {
        if self.processed > 0 {
            let mut sql = format!(
                "{}{}",
                &self.sql,
                self.items.iter().fold(String::new(), |t, it| format!(
                    "{}({}),\n",
                    t,
                    it.to_insert_values()
                ))
            );
            sql.pop();
            sql.pop();
            Some(sql)
        } else {
            None
        }
    }

    fn execute(
        &self,
        tx: &postgres::transaction::Transaction,
    ) -> postgres::Result<u64> {
        if let Some(sql) = self.get_sql() {
            tx.execute(&sql, &[])
        } else {
            Ok(0)
        }
    }

    fn execute_with(
        &self,
        tx: &postgres::transaction::Transaction,
        extra_sql: String,
    ) -> postgres::Result<u64> {
        if let Some(sql) = self.get_sql() {
            let full_sql = format!("{} {}", sql, extra_sql);
            tx.execute(&full_sql, &[])
        } else {
            Ok(0)
        }
    }
}
