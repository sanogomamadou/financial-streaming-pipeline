Write-Host "📝 Création des topics Kafka..." -ForegroundColor Green

$topics = @(
    @{Name="stocks.AAPL"; Partitions=3},
    @{Name="stocks.GOOGL"; Partitions=3},
    @{Name="stocks.MSFT"; Partitions=3},
    @{Name="stocks.TSLA"; Partitions=3},
    @{Name="stocks.AMZN"; Partitions=3},
    @{Name="forex.EURUSD"; Partitions=2},
    @{Name="forex.GBPUSD"; Partitions=2},
    @{Name="crypto.BTCUSD"; Partitions=2},
    @{Name="market.indicators"; Partitions=6},
    @{Name="trading.alerts"; Partitions=3}
)

foreach ($topic in $topics) {
    Write-Host "Création du topic: $($topic.Name)" -ForegroundColor Yellow
    docker exec kafka-broker kafka-topics `
        --create `
        --bootstrap-server localhost:9092 `
        --topic $topic.Name `
        --partitions $topic.Partitions `
        --replication-factor 1
}

Write-Host "`n✅ Tous les topics créés!" -ForegroundColor Green
Write-Host "`n📋 Liste des topics disponibles:" -ForegroundColor Cyan
docker exec kafka-broker kafka-topics --list --bootstrap-server localhost:9092