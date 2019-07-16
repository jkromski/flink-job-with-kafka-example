


# Kafka

## Create a topic

go to container:
```bash
docker exec -it flinkjobwithkafkaexample_kafka_1 /bin/bash
```

in folder: `/opt/kafka_2.11-0.10.1.0` run:
```bash
bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic test --partitions 1 --replication-factor 1
```
