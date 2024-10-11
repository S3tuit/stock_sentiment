#!/bin/bash

BROKER="broker:29092"

# Create Kafka topics in the cli-tools container
docker exec -it cli-tools kafka-topics --create \
  --bootstrap-server $BROKER \
  --replication-factor 1 \
  --partitions 1 \
  --topic test.articles_v2 \
  --config cleanup.policy=delete \
  --config flush.ms=9223372036854775807

docker exec -it cli-tools kafka-topics --create \
  --bootstrap-server $BROKER \
  --replication-factor 1 \
  --partitions 1 \
  --topic test.balance_sheet \
  --config cleanup.policy=delete \
  --config flush.ms=9223372036854775807

  docker exec -it cli-tools kafka-topics --create \
  --bootstrap-server $BROKER \
  --replication-factor 1 \
  --partitions 1 \
  --topic test.openai_sentiment \
  --config cleanup.policy=delete \
  --config flush.ms=9223372036854775807

  docker exec -it cli-tools kafka-topics --create \
  --bootstrap-server $BROKER \
  --replication-factor 1 \
  --partitions 1 \
  --topic test.price_info \
  --config cleanup.policy=delete \
  --config flush.ms=9223372036854775807

  docker exec -it cli-tools kafka-topics --create \
  --bootstrap-server $BROKER \
  --replication-factor 1 \
  --partitions 1 \
  --topic test.stock_sentiment \
  --config cleanup.policy=delete \
  --config flush.ms=9223372036854775807

echo "Kafka setup complete: Topics created."