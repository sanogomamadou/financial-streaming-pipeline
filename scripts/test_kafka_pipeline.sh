#!/bin/bash

echo "🧪 TEST COMPLET DU PIPELINE KAFKA"
echo "=================================="

# Configuration
KAFKA_BROKER="localhost:9092"
TEST_DURATION=30  # secondes

# Vérification que Kafka est démarré
echo "1. Vérification de l'état Kafka..."
docker-compose -f infrastructure/kafka/docker-compose-kafka.yml ps

# Test de connectivité
echo ""
echo "2. Test de connectivité Kafka..."
python data-producers/market_data_consumer.py --quick-test

# Démarrage du producteur en arrière-plan
echo ""
echo "3. Démarrage du producteur de données..."
cd data-producers
python market_data_producer.py &
PRODUCER_PID=$!
cd ..

# Attendre que le producteur démarre
sleep 5

# Lancement du test de validation
echo ""
echo "4. Lancement du test de validation ($TEST_DURATION secondes)..."
python data-producers/market_data_consumer.py --duration $TEST_DURATION

# Arrêt du producteur
echo ""
echo "5. Arrêt du producteur..."
kill $PRODUCER_PID 2>/dev/null

echo ""
echo "✅ Test terminé!"