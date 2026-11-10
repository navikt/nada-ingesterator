use crate::crd::debezium::DebeziumServer;
use crate::crd::ingesterator::Ingest;
use crate::generator::debezium::{DebeziumOpts, ReplicationConfig};
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use kube::api::{DeleteParams, PostParams};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, CustomResourceExt, Error, ResourceExt};
use sqlx::postgres::PgConnectOptions;
use std::time::Duration;
use tokio::time::sleep;
use tracing::log::{error, info, warn};

mod crd;
#[allow(unused)]
mod generator;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Print CustomResourceDefinition YAML for the `Ingest` resource to standard output.
    Crd,
    /// Run the Ingesterator Kubernetes operator.
    Run,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    match args.command {
        Commands::Crd => generate_crd()?,
        Commands::Run => run().await?,
    }

    Ok(())
}

fn generate_crd() -> Result<(), serde_yaml::Error> {
    let crd = Ingest::crd();
    let stdout = std::io::stdout().lock();
    serde_yaml::to_writer(stdout, &crd)?;
    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    info!("Ingesterator {}", env!("CARGO_PKG_VERSION"));

    info!("Connecting to Kubernetes...");
    let client = Client::try_default().await?;
    let ingests: Api<Ingest> = Api::all(client.clone());

    info!("Subscribing on changes to Ingest resources...");
    let wc = watcher::Config::default();
    let mut astream = watcher(ingests, wc).applied_objects().boxed();

    info!("Successfully initialized.");

    while let Some(ingest_resource) = astream.try_next().await? {
        // TODO: call to Nais API to get the following:
        // - Database IP address
        // - Username and password
        // - TLS parameters
        // - IP address of the Postgres server

        let replication_config = ReplicationConfig {
            username: "debezium_username".to_string(),
            password: "debezium_password".to_string(),
            database: ingest_resource.spec.postgres.clone(),
            schema: "public".to_string(),
            tables: vec![],
        };

        let postgres_options = PgConnectOptions::new_without_pgpass().database("app");

        let opts = DebeziumOpts {
            ingest: &ingest_resource,
            postgres_options,
            replication_config,
        };

        let debezium_resource = generator::debezium::new(opts);

        match save_to_kubernetes(client.clone(), &debezium_resource).await {
            Ok(_) => info!(
                "Created Debezium resource {}.{}",
                debezium_resource.metadata.namespace.unwrap(),
                debezium_resource.metadata.name.unwrap(),
            ),
            Err(err) => error!(
                "Create Debezium resource from Ingest {}.{}: {}",
                ingest_resource.metadata.namespace.unwrap(),
                ingest_resource.metadata.name.unwrap(),
                err.to_string(),
            ),
        };
    }

    Ok(())
}

async fn save_to_kubernetes(client: Client, resource: &DebeziumServer) -> Result<(), Error> {
    info!("Connecting to Kubernetes...");

    let debz: Api<DebeziumServer> = Api::namespaced(client, "kimtore");

    if let Some(name) = &resource.metadata.name {
        // copy some existing props over to the new object
        // https://cloudnative-pg.io/docs/1.30/replication/#streaming-replication-support
        if let Some(existing_resource) = debz.get_opt(&name).await? {
            warn!(
                "Deleting existing DebeziumServer '{}'...",
                existing_resource.name_any()
            );
            debz.delete(&name, &DeleteParams::foreground()).await?;
            sleep(Duration::from_millis(50)).await; // hack to prevent race condition
        }
    }

    info!("Creating DebeziumServer...");
    debz.create(&PostParams::default(), &resource).await?;

    Ok(())
}
