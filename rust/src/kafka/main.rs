extern crate exitcode;
extern crate ini;
extern crate klstore;
extern crate log;

use ini::Ini;
use klstore::*;
use std::env;
use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};
use std::time::Duration;

fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        log::error!("usage: {} <CONFIG_PATH>", &args[0]);
        std::process::exit(exitcode::CONFIG);
    }
    // load ini from file
    let cfg_path = &args[1];
    let cfg = match Ini::load_from_file(cfg_path) {
        Ok(v) => v,
        Err(err) => {
            log::error!("could not load {}: {}", cfg_path, err.to_string());
            std::process::exit(exitcode::CONFIG);
        }
    };

    // parse configurations from ini
    let s3_config = match S3StoreConfig::load(&cfg) {
        Ok(v) => {
            log::info!("{:#?}", &v);
            v
        }
        Err(err) => {
            log::error!("could not load s3 config: {}", err.to_string());
            std::process::exit(exitcode::CONFIG);
        }
    };
    let batcher_config = match BatchingStoreWriterConfig::load(&cfg) {
        Ok(v) => {
            log::info!("{:#?}", &v);
            v
        }
        Err(err) => {
            log::error!("could not load batcher config: {}", err.to_string());
            std::process::exit(exitcode::CONFIG);
        }
    };
    let kafka_config = match KafkaConsumerBridgeConfig::load(&cfg) {
        Ok(v) => {
            log::info!("{:#?}", &v);
            v
        }
        Err(err) => {
            log::error!("could not load kafka config: {}", err.to_string());
            std::process::exit(exitcode::CONFIG);
        }
    };

    // instantiate objects
    let s3 = match S3StoreWriter::new(s3_config) {
        Ok(v) => v,
        Err(err) => {
            log::error!("could not instantiate s3 writer: {}", err.to_string());
            std::process::exit(exitcode::SOFTWARE);
        }
    };
    let batcher = match BatchingStoreWriter::new(batcher_config, s3) {
        Ok(v) => v,
        Err(err) => {
            log::error!("could not instantiate batcher: {}", err.to_string());
            std::process::exit(exitcode::SOFTWARE);
        }
    };
    let kafka = match KafkaConsumerBridge::new(kafka_config, batcher) {
        Ok(v) => v,
        Err(err) => {
            log::error!("could not instantiate kafka bridge: {}", err.to_string());
            std::process::exit(exitcode::SOFTWARE);
        }
    };

    // register ctrlc handler
    log::info!("registering ctrlc handler");
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        log::info!("received ctrlc");
        r.store(false, Ordering::Relaxed);
    })
    .expect("could not set ctrlc handler");

    // subscrive
    log::info!("subscribing");
    match kafka.subscribe() {
        Ok(_) => {}
        Err(err) => {
            log::error!("could not subscrive to kafka topic: {}", err.to_string());
            std::process::exit(exitcode::SOFTWARE);
        }
    }

    // poll loop
    let poll_timeout = Duration::from_millis(100);
    log::info!("entering poll loop");
    while running.load(Ordering::Relaxed) {
        match kafka.poll(poll_timeout) {
            Ok(_) => {}
            Err(StoreError::IOError(s)) => {
                log::error!("IOError, Exiting: {}", s.to_string());
                std::process::exit(exitcode::IOERR);
            }
            Err(err) => {
                log::error!("{}", err.to_string());
            }
        }
    }
    log::info!("exiting")
}
