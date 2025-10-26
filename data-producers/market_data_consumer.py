import json
import time
from kafka import KafkaConsumer
from datetime import datetime, timezone  # CHANGEMENT IMPORTANT ICI
import threading
import logging
from collections import defaultdict, Counter

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.metrics = {
            'message_counts': Counter(),
            'last_prices': {},
            'throughput': defaultdict(list),
            'latencies': defaultdict(list)
        }
        self.running = False
        
        # Topics à monitorer
        self.topics = [
            'stocks.AAPL', 'stocks.GOOGL', 'stocks.MSFT', 
            'stocks.TSLA', 'stocks.AMZN', 'forex.EURUSD', 
            'forex.GBPUSD', 'crypto.BTCUSD', 'market.indicators', 
            'trading.alerts'
        ]
    
    def create_consumer(self, topic, group_id_suffix=""):
        """Crée un consumer Kafka pour un topic spécifique"""
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f"test_consumer_{group_id_suffix}_{topic}",
            consumer_timeout_ms=10000
        )
    
    def parse_timestamp(self, timestamp_str):
        """Parse le timestamp en gérant correctement les timezones"""
        try:
            # Gestion des différents formats de timestamp
            if timestamp_str.endswith('Z'):
                # Format ISO avec Z (UTC)
                return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                # Autres formats
                return datetime.fromisoformat(timestamp_str)
        except Exception as e:
            logger.warning(f"Erreur parsing timestamp {timestamp_str}: {e}")
            return datetime.now(timezone.utc)  # Fallback
    
    def calculate_latency(self, message_timestamp):
        """Calcule la latence en gérant les timezones"""
        try:
            # Utiliser datetime.now avec timezone UTC
            current_time = datetime.now(timezone.utc)
            
            # S'assurer que les deux datetime ont une timezone
            if message_timestamp.tzinfo is None:
                message_timestamp = message_timestamp.replace(tzinfo=timezone.utc)
            
            # Calculer la différence
            latency = (current_time - message_timestamp).total_seconds() * 1000  # en ms
            return latency
        except Exception as e:
            logger.warning(f"Erreur calcul latence: {e}")
            return 0.0
    
    def consume_topic(self, topic):
        """Consomme les messages d'un topic spécifique"""
        logger.info(f"Démarrage du consumer pour le topic: {topic}")
        
        try:
            consumer = self.create_consumer(topic, "validation")
            message_count = 0
            
            for message in consumer:
                if not self.running:
                    break
                    
                message_count += 1
                self.metrics['message_counts'][topic] += 1
                
                # Calcul de la latence (si le timestamp est présent)
                if 'timestamp' in message.value:
                    message_time = self.parse_timestamp(message.value['timestamp'])
                    latency = self.calculate_latency(message_time)
                    self.metrics['latencies'][topic].append(latency)
                
                # Stocker le dernier prix pour les topics de market data
                if topic.startswith(('stocks.', 'forex.', 'crypto.')) and 'price' in message.value:
                    symbol = message.value.get('symbol', 'unknown')
                    self.metrics['last_prices'][symbol] = message.value['price']
                
                # Afficher le message (limité pour éviter le spam)
                if message_count <= 3:  # Afficher seulement les 3 premiers messages
                    logger.info(f"📨 [{topic}] Message #{message_count}: {message.value}")
                elif message_count == 4:
                    logger.info(f"📨 [{topic}] ... (messages supplémentaires non affichés)")
                
                # Mettre à jour le throughput toutes les 10 messages
                if message_count % 10 == 0:
                    current_minute = datetime.now(timezone.utc).strftime('%H:%M')
                    self.metrics['throughput'][topic].append((current_minute, 10))
            
            consumer.close()
            logger.info(f"Consumer arrêté pour {topic}. Total messages: {message_count}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la consommation du topic {topic}: {e}")
    
    def start_all_consumers(self):
        """Démarre un thread de consommation pour chaque topic"""
        self.running = True
        threads = []
        
        logger.info("🚀 Démarrage de tous les consumers de test...")
        
        for topic in self.topics:
            thread = threading.Thread(target=self.consume_topic, args=(topic,))
            thread.daemon = True
            threads.append(thread)
            thread.start()
            time.sleep(0.1)  # Petit délai entre chaque démarrage
        
        return threads
    
    def print_real_time_dashboard(self):
        """Affiche un dashboard temps réel des métriques"""
        try:
            while self.running:
                print("\n" + "="*80)
                print("📊 DASHBOARD TEMPS RÉEL - KAFKA CONSUMER")
                print("="*80)
                
                # Messages par topic
                print("\n📨 MESSAGES PAR TOPIC:")
                for topic, count in self.metrics['message_counts'].most_common():
                    print(f"   {topic}: {count} messages")
                
                # Derniers prix
                if self.metrics['last_prices']:
                    print("\n💹 DERNIERS PRIX:")
                    for symbol, price in sorted(self.metrics['last_prices'].items()):
                        if isinstance(price, (int, float)):
                            print(f"   {symbol}: {price:.4f}")
                        else:
                            print(f"   {symbol}: {price}")
                
                # Latences moyennes
                print("\n⏱️  LATENCES MOYENNES (ms):")
                for topic, latencies in self.metrics['latencies'].items():
                    if latencies:
                        avg_latency = sum(latencies) / len(latencies)
                        print(f"   {topic}: {avg_latency:.2f}ms")
                
                # Throughput récent
                print("\n📈 THROUGHPUT (dernière minute):")
                for topic, throughput_data in self.metrics['throughput'].items():
                    if throughput_data:
                        recent_count = sum(count for minute, count in throughput_data[-6:])  # 60s
                        print(f"   {topic}: {recent_count} msg/min")
                
                print(f"\n🕐 Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}")
                print("="*80)
                
                time.sleep(10)  # Rafraîchissement toutes les 10 secondes
                
        except KeyboardInterrupt:
            logger.info("Dashboard arrêté")
    
    def run_validation_test(self, duration=60):
        """Exécute un test de validation complet"""
        logger.info(f"🧪 Démarrage du test de validation pour {duration} secondes")
        
        # Démarrer tous les consumers
        threads = self.start_all_consumers()
        
        # Démarrer le dashboard
        dashboard_thread = threading.Thread(target=self.print_real_time_dashboard)
        dashboard_thread.daemon = True
        dashboard_thread.start()
        
        try:
            # Attendre la durée spécifiée
            time.sleep(duration)
            
            # Arrêter proprement
            self.running = False
            logger.info("Test de validation terminé")
            
            # Générer un rapport final
            self.generate_final_report()
            
        except KeyboardInterrupt:
            logger.info("Test interrompu par l'utilisateur")
            self.running = False
    
    def generate_final_report(self):
        """Génère un rapport détaillé du test"""
        print("\n" + "="*80)
        print("📋 RAPPORT FINAL DE VALIDATION KAFKA")
        print("="*80)
        
        total_messages = sum(self.metrics['message_counts'].values())
        print(f"\n📊 TOTAL DES MESSAGES TRAITÉS: {total_messages}")
        
        print("\n📨 DÉTAIL PAR TOPIC:")
        for topic, count in self.metrics['message_counts'].most_common():
            percentage = (count / total_messages * 100) if total_messages > 0 else 0
            print(f"   {topic}: {count} messages ({percentage:.1f}%)")
        
        print("\n⏱️  PERFORMANCES:")
        for topic, latencies in self.metrics['latencies'].items():
            if latencies:
                avg_latency = sum(latencies) / len(latencies)
                max_latency = max(latencies)
                min_latency = min(latencies)
                print(f"   {topic}:")
                print(f"      Moyenne: {avg_latency:.2f}ms")
                print(f"      Min: {min_latency:.2f}ms")
                print(f"      Max: {max_latency:.2f}ms")
                print(f"      Échantillons: {len(latencies)}")
        
        # Vérification de la santé des données
        print("\n✅ VÉRIFICATION DE LA SANTÉ DES DONNÉES:")
        healthy = True
        
        for topic in self.topics:
            if topic in self.metrics['message_counts']:
                count = self.metrics['message_counts'][topic]
                if count > 0:
                    print(f"   ✓ {topic}: DONNÉES DÉTECTÉES ({count} messages)")
                else:
                    print(f"   ⚠ {topic}: TOPIC VIDE (mais accessible)")
                    healthy = False
            else:
                print(f"   ✗ {topic}: TOPIC NON ACCÉSSIBLE")
                healthy = False
        
        if healthy:
            print("\n🎉 TOUS LES TESTS SONT PASSÉS AVEC SUCCÈS!")
        else:
            print("\n⚠️  CERTAINS TESTS ONT ÉCHOUÉ - VÉRIFIEZ LA CONFIGURATION KAFKA")

def quick_test():
    """Test rapide pour vérifier la connectivité Kafka"""
    print("🧪 TEST RAPIDE DE CONNECTIVITÉ KAFKA")
    
    try:
        # Test de connexion basique
        consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='quick_test',
            consumer_timeout_ms=5000
        )
        
        topics = consumer.topics()
        print(f"✅ Connecté à Kafka - Topics disponibles: {len(topics)}")
        
        for topic in sorted(topics):
            partitions = consumer.partitions_for_topic(topic)
            print(f"   📁 {topic} ({len(partitions)} partitions)")
        
        consumer.close()
        
    except Exception as e:
        print(f"❌ Erreur de connexion Kafka: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Consumer de test pour données de marché Kafka')
    parser.add_argument('--quick-test', action='store_true', help='Test rapide de connectivité')
    parser.add_argument('--duration', type=int, default=60, help='Durée du test en secondes')
    parser.add_argument('--topic', type=str, help='Test un topic spécifique seulement')
    
    args = parser.parse_args()
    
    if args.quick_test:
        quick_test()
    else:
        consumer = MarketDataConsumer()
        
        if args.topic:
            # Test d'un seul topic
            logger.info(f"Test du topic spécifique: {args.topic}")
            consumer.topics = [args.topic]
            consumer.consume_topic(args.topic)
        else:
            # Test complet
            consumer.run_validation_test(duration=args.duration)