use crate::debezium::DebeziumServer;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{ListParams, PostParams};
use kube::{Api, Client, Error};
use tracing::log::info;

pub async fn init() -> Result<(), Error> {
    info!("Connecting to Kubernetes...");
    let client = Client::try_default().await?;
    info!("Kubernetes is online.");

    let debz: Api<DebeziumServer> = Api::namespaced(client, "kimtore");

    let metadata = ObjectMeta {
        name: Some("kimtore".into()),
        namespace: Some("kimtore".into()),
        ..Default::default()
    };

    let config = DebeziumServer {
        metadata,
        spec: Default::default(),
    };

    debz.create(&PostParams::default(), &config).await?;

    // let resources = debz.list(&ListParams::default()).await?;
    // for res in resources {
    //     info!("{:?}", res);
    // }

    Ok(())
}
