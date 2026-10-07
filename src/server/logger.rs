use chrono::Duration;
use file_rotate::{
    compression::Compression,
    suffix::{AppendTimestamp, FileLimit},
    ContentLimit, FileRotate,
};
use std::sync::Mutex;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use crate::server::utils::config::{self};

pub fn init() {
    println!("[Logger] Initializing logger...");

    let log_level = config::GLOBAL_CONFIG
        .get::<u16>("logger.level")
        .unwrap_or(0);

    let log_level = match log_level {
        0 => Level::TRACE,
        1 => Level::DEBUG,
        2 => Level::INFO,
        3 => Level::WARN,
        4 => Level::ERROR,
        _ => Level::DEBUG,
    };

    match config::get_runtime_environment() {
        config::RuntimeEnvironment::Production => {
            config_tracing_for_production(log_level);
        }
        _ => {
            let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("[Logger] Setting global default subscriber failed for tracing");
        }
    }
}

fn config_tracing_for_production(log_level: Level) {
    let log_file_prefix = "log";
    let log_dir = "./logs/";
    let writer = FileRotate::new(
        log_dir.to_string() + log_file_prefix,
        AppendTimestamp::default(FileLimit::Age(Duration::days(30))), // Keep logs for 30 days
        ContentLimit::BytesSurpassed(1024 * 1024 * 5),                // 5 MB
        Compression::OnRotate(2), // Compress all files except the last two
        None,
    );

    let log_format = tracing_subscriber::fmt::format()
        .with_thread_ids(true)
        .with_level(true)
        .with_source_location(true)
        .json();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(log_level)
        .event_format(log_format)
        .with_writer(Mutex::new(writer))
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("[Logger] Setting global default subscriber failed for tracing");
}
