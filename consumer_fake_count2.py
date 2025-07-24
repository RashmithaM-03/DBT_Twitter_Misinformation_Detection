from kafka import KafkaConsumer
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'fake-tweets-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Tumbling window of 15 minutes
window_start = datetime.now()
window_duration = timedelta(minutes=15)
fake_count = 0

print("🕒 Starting Fake Tweet Detection with Tumbling Window (15 min)...")

# Loop indefinitely over the dataset
while True:
    for msg in consumer:
        tweet = msg.value
        timestamp = datetime.strptime(tweet['timestamp'], '%Y-%m-%d %H:%M:%S')

        if tweet['label'].lower() == 'fake':
            fake_count += 1

        now = datetime.now()
        if now - window_start >= window_duration:
            print(f"\n⏰ {window_start.strftime('%H:%M:%S')} - {now.strftime('%H:%M:%S')}")
            print(f"⚠️  Fake tweets detected: {fake_count}")
            print("-" * 40)

            # Reset window
            window_start = now
            fake_count = 0
            
    # Reiterate over the dataset once all messages are processed
    print("Reiterating over dataset...")
    consumer.seek_to_beginning()

