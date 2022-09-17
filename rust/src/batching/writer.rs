use crate::common::time::*;
use crate::*;
use linked_hash_map::LinkedHashMap;
use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use threadlanes::{LaneExecutor, ThreadLanes};

struct Batch {
    inserts: Vec<Insertion>,
    size: u64,
    flush_timestamp: u64,
}
enum Task {
    Append(String, String, Vec<Insertion>),
    FlushKey(String, String),
    FlushAll,
    CheckWrite,
}
struct TaskExecutor<W: StoreWriter> {
    writer: W,
    batches: LinkedHashMap<(String, String), Batch>,
    batch_flush_interval_millis: u64,
    batch_flush_record_count_threshold: u64,
    batch_flush_size_threshold: u64,
}
impl<W: StoreWriter> TaskExecutor<W> {
    fn new(writer: W, config: &BatchingStoreWriterConfig) -> Self {
        Self {
            writer,
            batches: LinkedHashMap::new(),
            batch_flush_interval_millis: config.batch_flush_interval_millis,
            batch_flush_record_count_threshold: config.batch_flush_record_count_threshold,
            batch_flush_size_threshold: config.batch_flush_size_threshold,
        }
    }
}
impl<W: StoreWriter> LaneExecutor<Task> for TaskExecutor<W> {
    fn execute(&mut self, task: Task) {
        match task {
            Task::Append(keyspace, key, mut inserts) => {
                // set None timestamps now
                let now = time_now_as_millis();
                for insert in &mut inserts {
                    if let None = insert.timestamp {
                        insert.timestamp = Some(now as i64);
                    }
                }
                // handle batch
                let batch_key = (keyspace, key);
                let batch_size = (&inserts).iter().map(|e| e.record.len() as u64).sum();
                match self.batches.get_mut(&batch_key) {
                    Some(batch) => {
                        // append to existing batch
                        batch.inserts.append(&mut inserts);
                        batch.size += batch_size;
                        // check if batch should be written now due to count threshold
                        if batch.inserts.len() as u64 >= self.batch_flush_record_count_threshold
                            || batch.size >= self.batch_flush_size_threshold
                        {
                            // write now
                            let entry = self.batches.pop_front().unwrap();
                            self.writer
                                .append(&entry.0 .0, &entry.0 .1, entry.1.inserts)
                                .expect("append failed");
                        }
                    }
                    None => {
                        if self.batch_flush_interval_millis == 0
                            || inserts.len() as u64 >= self.batch_flush_record_count_threshold
                            || batch_size >= self.batch_flush_size_threshold
                        {
                            // write now
                            self.writer
                                .append(&batch_key.0, &batch_key.1, inserts)
                                .expect("append failed");
                        } else {
                            // start new batch
                            self.batches.insert(
                                batch_key,
                                Batch {
                                    flush_timestamp: now + self.batch_flush_interval_millis,
                                    size: batch_size,
                                    inserts: inserts,
                                },
                            );
                        }
                    }
                }
            }
            Task::FlushKey(keyspace, key) => {
                let batch_key = (keyspace, key);
                if let Some(batch) = self.batches.remove(&batch_key) {
                    self.writer
                        .append(&batch_key.0, &batch_key.1, batch.inserts)
                        .expect("append failed");
                }
            }
            Task::FlushAll => {
                while !self.batches.is_empty() {
                    let entry = self.batches.pop_front().unwrap();
                    self.writer
                        .append(&entry.0 .0, &entry.0 .1, entry.1.inserts)
                        .expect("append failed");
                }
            }
            Task::CheckWrite => {
                // check if batches should be written due to time threshold
                let now = time_now_as_millis();
                while !self.batches.is_empty()
                    && now > self.batches.front().unwrap().1.flush_timestamp
                {
                    let entry = self.batches.pop_front().unwrap();
                    self.writer
                        .append(&entry.0 .0, &entry.0 .1, entry.1.inserts)
                        .expect("append failed");
                }
            }
        }
    }
}

pub struct BatchingStoreWriter<W: StoreWriter> {
    writer: W,
    writer_thread_count: u64,
    batch_check_interval_millis: u64,
    next_batch_check: RefCell<u64>,
    thread_lanes: ThreadLanes<Task>,
}
impl<W: StoreWriter + Clone + Send + 'static> BatchingStoreWriter<W> {
    pub fn new(config: BatchingStoreWriterConfig, writer: W) -> Result<Self, StoreError> {
        let mut executors: Vec<TaskExecutor<W>> = Vec::new();
        for _ in 0..config.writer_thread_count {
            executors.push(TaskExecutor::new(writer.clone(), &config));
        }
        Ok(Self {
            writer,
            writer_thread_count: config.writer_thread_count as u64,
            batch_check_interval_millis: config.batch_check_interval_millis,
            next_batch_check: RefCell::new(
                time_now_as_millis() + config.batch_check_interval_millis,
            ),
            thread_lanes: ThreadLanes::new(executors, config.writer_thread_queue_capacity),
        })
    }
}
impl<W: StoreWriter> StoreWriter for BatchingStoreWriter<W> {
    fn create_keyspace(&self, keyspace: &str) -> Result<CreatedKeyspace, StoreError> {
        self.writer.create_keyspace(keyspace)
    }
    fn append(
        &self,
        keyspace: &str,
        key: &str,
        inserts: Vec<Insertion>,
    ) -> Result<(), StoreError> {
        self.thread_lanes.send(
            lane(keyspace, key, self.writer_thread_count),
            Task::Append(keyspace.to_string(), key.to_string(), inserts),
        );
        self.duty_cycle()?;
        Ok(())
    }
    fn flush_key(&self, keyspace: &str, key: &str) -> Result<(), StoreError> {
        self.thread_lanes.send(
            lane(keyspace, key, self.writer_thread_count),
            Task::FlushKey(keyspace.to_string(), key.to_string()),
        );
        self.thread_lanes
            .flush_lane(lane(keyspace, key, self.writer_thread_count));
        Ok(())
    }
    fn flush_all(&self) -> Result<(), StoreError> {
        for lane in 0..self.writer_thread_count {
            self.thread_lanes.send(lane as usize, Task::FlushAll);
        }
        self.thread_lanes.flush();
        Ok(())
    }
    fn duty_cycle(&self) -> Result<(), StoreError> {
        let now = time_now_as_millis();
        if now >= *self.next_batch_check.borrow() {
            for lane in 0..self.writer_thread_count {
                self.thread_lanes.send(lane as usize, Task::CheckWrite);
            }
            self.next_batch_check
                .replace(now + self.batch_check_interval_millis);
        }
        Ok(())
    }
}

fn lane(keyspace: &str, key: &str, thread_count: u64) -> usize {
    let mut s = DefaultHasher::new();
    keyspace.hash(&mut s);
    key.hash(&mut s);
    let hash = s.finish();
    return (hash % thread_count) as usize;
}
