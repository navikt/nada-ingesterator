// apiVersion: debezium.io/v1alpha1
// kind: DebeziumServer
// metadata:
// name: my-debezium-server
// spec:
// version: "3.2.0"
// source: {{postgres_conf}}
// sink: "{{kafka conf, tror noe ca '{type: kafka, config: {bootstrap.servers: kafka:9092}'}"
// format: {{format_conf, mener avro greier går her}}
// runtime: {{runtime_conf, tror vi kan komme unna med default på det meste, men må antagelig confe otel}}
// transforms: {{ transformation_conf, muligens trenger vi å håndtere tombstone events }}
// predicates: {{predicate_conf, trenger antagelig ikke dette, mulig å filtrere events}}
// quarkus: {{quarkus greier som sendes til vel quarkus}}

// plugin.name: pgoutput
// slot.name: TBD # matcher det vi setter opp ved bootstrap
// slot.failover: ? # noe greier her med pg <= 16
// publication.name: TBD # må matche bootstrap
// database.*: <db params>
// topic.prefix: ? # kansje, men må aldri endre en man har satt
// [schema|table|column].[include|exclude].list: []
// database.ssl*: ?
// message.key.columns: ? # muligens ved composite keys
// publication.autocreate.mode: disabled # all_tables som er default trenger superuser
// heartbeat.interval.ms: ? # må nok justere oss inn etter hvert, starte med hvert 10 sek?
// heartbeat.action.query: "INSERT INTO dbz_heartbeat_table (text) VALUES ('dbz_heartbeat')"
// lsn.flush.mode: ? # er preview men vi kunne muligens slippe heartbeat
// read.only: # må muligens se på den avhengig av rettigheter vi får