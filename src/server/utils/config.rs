use config::Config;
use std::env;
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeEnvironment {
    Development,
    Production,
}

pub static GLOBAL_CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let config_file = env::var("CONFIG_FILE").unwrap_or("config_dev.json".to_string());
    println!("[CONFIG] config_file :: {}", config_file);
    Config::builder()
        .add_source(config::File::with_name(&config_file))
        .build()
        .expect("[CONFIG] Error processing config file")
});

/// Reads a config string, falling back to `default` when the key is missing or set to "".
///
/// An empty string in the JSON means "use the built-in path", so a config file can list every
/// key for discoverability without having to carry the real defaults.
pub fn get_string_or(key: &str, default: &str) -> String {
    match GLOBAL_CONFIG.get::<String>(key) {
        Ok(value) if !value.trim().is_empty() => value,
        _ => default.to_string(),
    }
}

pub fn get_runtime_environment() -> RuntimeEnvironment {
    let env = GLOBAL_CONFIG.get::<String>("environment").unwrap();
    match env.as_str() {
        "production" => RuntimeEnvironment::Production,
        "development" => RuntimeEnvironment::Development,
        _ => RuntimeEnvironment::Development,
    }
}
