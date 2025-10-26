#!/bin/bash

KAFKA_HOME=${KAFKA_HOME:-"/opt/kafka"}
BROKER="localhost:9092"

echo "Création des topics Kafka pour le pipeline financier..."

# Topics pour les actions
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic stocks.AAPL \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete

$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic stocks.GOOGL \
    --partitions 3 \
    --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic stocks.MSFT \
    --partitions 3 \
    --replication-factor 1

# Topics pour le Forex
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic forex.EURUSD \
    --partitions 2 \
    --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic forex.GBPUSD \
    --partitions 2 \
    --replication-factor 1

# Topic pour les cryptos
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic crypto.BTCUSD \
    --partitions 2 \
    --replication-factor 1

# Topic pour les alertes et résultats de traitement
$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic market.indicators \
    --partitions 6 \
    --replication-factor 1

$KAFKA_HOME/bin/kafka-topics.sh --create \
    --bootstrap-server $BROKER \
    --topic trading.alerts \
    --partitions 3 \
    --replication-factor 1

echo "Liste de tous les topics créés:"
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BROKER