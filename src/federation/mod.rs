pub mod postgres_full_pushdown;

pub async fn init_federation() -> datafusion::error::Result<()> {
    // Try to pushdown as much as possible to the remote postgres query engine:
    let _ = postgres_full_pushdown::init_federated_pushdown().await;
    Ok(())
}
