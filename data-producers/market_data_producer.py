import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import yaml
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        # Configuration Kafka
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Garantie de livraison
            retries=3,
            batch_size=16384,
            linger_ms=10
        )
        
        # Symbols boursiers simulés
        self.symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'EURUSD', 'GBPUSD', 'BTCUSD']
        
        # Prix de référence pour chaque symbole
        self.reference_prices = {
            'AAPL': 150.0, 'GOOGL': 2700.0, 'MSFT': 300.0, 
            'TSLA': 200.0, 'AMZN': 3300.0, 'EURUSD': 1.08, 
            'GBPUSD': 1.26, 'BTCUSD': 45000.0
        }
        
        self.volatilities = {
            'AAPL': 0.02, 'GOOGL': 0.015, 'MSFT': 0.018,
            'TSLA': 0.035, 'AMZN': 0.025, 'EURUSD': 0.008,
            'GBPUSD': 0.009, 'BTCUSD': 0.05
        }
    
    def generate_market_data(self, symbol):
        """Génère un tick de marché réaliste avec modèle de prix aléatoire"""
        base_price = self.reference_prices[symbol]
        volatility = self.volatilities[symbol]
        
        # Mouvement de prix géométrique brownien
        price_change = random.gauss(0, volatility * 0.01)
        new_price = base_price * (1 + price_change)
        
        # Mise à jour du prix de référence
        self.reference_prices[symbol] = new_price
        
        # Génération du volume (aléatoire mais réaliste)
        volume = random.randint(100, 10000)
        
        # Spread bid-ask réaliste
        spread = new_price * 0.0002  # 2 basis points
        bid_price = new_price - spread / 2
        ask_price = new_price + spread / 2
        
        tick_data = {
            'symbol': symbol,
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'price': round(new_price, 4),
            'bid': round(bid_price, 4),
            'ask': round(ask_price, 4),
            'volume': volume,
            'exchange': 'NASDAQ' if symbol in ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'] else 'FOREX' if 'USD' in symbol else 'CRYPTO',
            'message_type': 'trade'
        }
        
        return tick_data
    
    def start_producing(self, ticks_per_second=100):
        """Démarre la production de données de marché"""
        logger.info(f"Démarrage de la production: {ticks_per_second} ticks/seconde")
        
        try:
            while True:
                start_time = time.time()
                ticks_generated = 0
                
                # Génération des ticks pour cette seconde
                while ticks_generated < ticks_per_second:
                    symbol = random.choice(self.symbols)
                    market_data = self.generate_market_data(symbol)
                    
                    # Envoi vers le topic Kafka approprié
                    topic = f"stocks.{symbol}" if symbol in ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'] else \
                    f"crypto.{symbol}" if symbol in ['BTCUSD', 'ETHUSD'] else \
                    f"forex.{symbol}" if 'USD' in symbol else f"crypto.{symbol}"

                    
                    self.producer.send(topic, market_data)
                    ticks_generated += 1
                    
                    # Petit délai pour étaler les envois dans la seconde
                    if ticks_generated < ticks_per_second:
                        time.sleep(1.0 / ticks_per_second)
                
                # Attente pour respecter le débit cible
                elapsed = time.time() - start_time
                if elapsed < 1.0:
                    time.sleep(1.0 - elapsed)
                    
                logger.info(f"Produit {ticks_generated} ticks - Prix AAPL: {self.reference_prices['AAPL']:.2f}")
                
        except KeyboardInterrupt:
            logger.info("Arrêt de la production...")
        finally:
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    producer = MarketDataProducer()
    producer.start_producing(ticks_per_second=50)  # 50 ticks/seconde pour débuter