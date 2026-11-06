use crate::crd::ingesterator::Ingest;
use clap::{Parser, Subcommand};
use futures::{StreamExt, TryStreamExt};
use kube::runtime::{WatchStreamExt, watcher};
use kube::{Api, Client, CustomResourceExt};
use tracing::log::info;

mod crd;
#[allow(unused)]
mod kubernetes;

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
    let ingests: Api<Ingest> = Api::all(client);

    info!("Creating watcher...");
    let wc = watcher::Config::default();
    let mut astream = watcher(ingests, wc).applied_objects().boxed();

    info!("Successfully initialized.");
    info!("Watching for changes on Ingest objects until terminated.");

    while let Some(objekt) = astream.try_next().await? {
        info!("Saw object: {objekt:?}")
    }

    Ok(())
}
