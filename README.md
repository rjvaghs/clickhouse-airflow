## Clickhouse Kafka Connect Sink Commands

List all the connectors: curl -X GET "http://localhost:8083/connectors
Start the connector: curl -X POST -H "Content-Type: application/json" --data @clickhouse-sink.json http://localhost:8083/connectors
Pause the connector: curl -X PUT localhost:8083/connectors/clickhouse-sink-connector/pause 
Delete the connector: curl -X DELETE localhost:8083/connectors/clickhouse-sink-connector
Status of the connector: curl -X GET http://localhost:8083/connectors/clickhouse-sink-connector/status

kafka-consumer-groups --bootstrap-server localhost:9092 --group connect-clickhouse-sink-connector --reset-offsets --to-earliest --topic employees_cdc --execute

kafka-topics --bootstrap-server localhost:9092 --create --topic docker-connect-offsets --partitions 1 --replication-factor 1 --config cleanup.policy=compact

kafka-topics --bootstrap-server localhost:9092 --create --topic docker-connect-configs --partitions 1 --replication-factor 1 --config cleanup.policy=compact

kafka-topics --bootstrap-server localhost:9092 --create --topic docker-connect-status --partitions 1 --replication-factor 1 --config cleanup.policy=compact

Need to configure config.xml to set a custom safe directory for files for file load engine to read data from the files