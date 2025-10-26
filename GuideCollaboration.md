## Guide à suivre pour évoluer

# 🏦 Pipeline de Données Financières Temps Réel

Chaque équipe devra ajouter ici les étapes qu'elle aura fait puis décrire les configurations à mettre en place pour permettre à l'équipe suivante de continuer sans problèmes 

## 🏗️ Architecture à suivre
```bash
financial-streaming-pipeline/
├── docker-compose.yml
├── infrastructure/
│   ├── kafka/
│   │   ├── docker-compose-kafka.yml
│   │   └── topics/
│   │       └── create-topics.sh
│   ├── flink/
│   │   └── docker-compose-flink.yml
│   ├── iceberg/
│   │   └── docker-compose-iceberg.yml
│   ├── trino/
│   │   └── docker-compose-trino.yml
│   └── superset/
│       └── docker-compose-superset.yml
├── data-producers/
│   ├── market_data_producer.py
│   ├── requirements.txt
│   └── config/
│       └── kafka_config.yaml
├── flink-jobs/
│   ├── src/
│   │   ├── main/
│   │   │   ├── scala/
│   │   │   │   └── com/
│   │   │   │       └── finance/
│   │   │   │           └── streaming/
│   │   │   │               ├── MarketDataProcessor.scala
│   │   │   │               ├── VolatilityCalculator.scala
│   │   │   │               └── AnomalyDetector.scala
│   │   │   └── resources/
│   │   │       └── log4j.properties
│   │   ├── build.sbt
│   │   └── project/
│   │       └── build.properties
│   └── sql/
│       ├── real_time_indicators.sql
│       └── volatility_calculation.sql
├── ml-models/
│   ├── train_volatility_model.py
│   ├── requirements-ml.txt
│   └── models/
│       └── model_metadata.json
├── trino-queries/
│   ├── market_analysis.sql
│   └── backtesting_queries.sql
├── superset-dashboards/
│   ├── dashboard_export.json
│   └── alerts_config.yaml
└── docs/
    ├── architecture.md
    └── deployment_guide.md
```
## 🚀 Démarrage Rapide

### Prérequis
- Docker & Docker Compose
- Python 3.8+
- Git

### 1. Cloner le projet
```bash
git clone https://github.com/sanogomamadou/financial-streaming-pipeline.git
```

### 2. Démarrer l'infrastructure
```bash
docker-compose -f infrastructure/kafka/docker-compose-kafka.yml up -d
./scripts/create-kafka-topics.ps1
```

### 3. Tester le pipeline
```bash
# Terminal 1 - Producteur
python data-producers/market_data_producer.py

# Terminal 2 - Consumer de test  
python data-producers/market_data_consumer.py --duration 30
```
