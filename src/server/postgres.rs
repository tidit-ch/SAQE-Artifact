use crate::server::utils::config;
use deadpool_postgres::{Config, Object, Pool, PoolError, RecyclingMethod, Runtime};
use std::sync::LazyLock;
use tokio_postgres::NoTls;

// TODO: Read how the retries are handled in the deadpool_postgres library
static DB_POOL_INSTANCE: LazyLock<Pool> = LazyLock::new(|| {
    let mut cfg = Config::new();
    cfg.dbname = Some(config::GLOBAL_CONFIG.get::<String>("postgres.db").unwrap());
    cfg.host = Some(
        config::GLOBAL_CONFIG
            .get::<String>("postgres.host")
            .unwrap(),
    );
    cfg.password = Some(
        config::GLOBAL_CONFIG
            .get::<String>("postgres.password")
            .unwrap(),
    );
    cfg.user = Some(
        config::GLOBAL_CONFIG
            .get::<String>("postgres.user")
            .unwrap(),
    );
    cfg.port = Some(config::GLOBAL_CONFIG.get::<u16>("postgres.port").unwrap());

    cfg.manager = Some(deadpool_postgres::ManagerConfig {
        recycling_method: RecyclingMethod::Verified,
    });

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    pool
});

pub async fn get_client() -> Result<Object, PoolError> {
    DB_POOL_INSTANCE.get().await
}
