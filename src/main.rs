use crate::crd::ingesterator::Ingest;
use crate::generator::debezium::{DebeziumOpts, GeneratedResources, ReplicationConfig};
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use k8s_openapi::NamespaceResourceScope;
use kube::api::{DeleteParams, PostParams};
use kube::runtime::reflector::Lookup;
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, CustomResourceExt, Error, Resource};
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::postgres::PgConnectOptions;
use std::fmt::Debug;
use std::time::Duration;
use tokio::time::sleep;
use tracing::log::{debug, error, info, warn};

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

        let resources = opts.generate();

        match save_all_to_kubernetes(client.clone(), &resources).await {
            Ok(_) => info!(
                "Created Debezium resource {}.{}",
                &resources.debezium_server.metadata.namespace.unwrap(),
                &resources.debezium_server.metadata.name.unwrap(),
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

async fn save_all_to_kubernetes(
    client: Client,
    resources: &GeneratedResources,
) -> Result<(), Error> {
    save_to_kubernetes_dyn(client.clone(), &resources.secret).await?;
    save_to_kubernetes_dyn(client.clone(), &resources.debezium_server).await?;
    Ok(())
}

/// Forcefully save a resource to Kubernetes.
///
/// If the object exists, it is deleted and replaced with a new resource.
async fn save_to_kubernetes_dyn<T>(client: Client, resource: &T) -> Result<(), Error>
where
    T: Resource<Scope = NamespaceResourceScope> + Serialize + Clone + Debug + DeserializeOwned,
    <T as kube::Resource>::DynamicType: Default,
{
    let name = resource.name().unwrap_or_default();
    let namespace = resource.namespace().unwrap_or_default();
    let debz: Api<T> = Api::namespaced(client, &namespace);

    debug!("Creating X resource '{}.{}'...", namespace, name);

    if let Some(name) = resource.name() {
        // copy some existing props over to the new object
        // https://cloudnative-pg.io/docs/1.30/replication/#streaming-replication-support
        if let Some(existing_resource) = debz.get_opt(&name).await? {
            warn!(
                "Deleting existing resource '{}'...",
                existing_resource.name().unwrap_or_default().to_string(),
            );
            debz.delete(&name, &DeleteParams::foreground()).await?;
            sleep(Duration::from_millis(50)).await; // hack to prevent race condition
        }
    }

    debz.create(&PostParams::default(), resource).await?;

    info!("Created X resource '{}.{}'", namespace, name);

    Ok(())
}
