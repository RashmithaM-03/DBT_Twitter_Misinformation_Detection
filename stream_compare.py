from kafka import KafkaConsumer
import json
from datetime import datetime

# Initialize consumer
consumer = KafkaConsumer(
    'fake-tweets-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Set up time tracking
start_time = datetime.now()
tweet_count = 0
fake_count = 0

print("🕒 Starting Fake Tweet Detection with Streaming Analysis...")

for msg in consumer:
    tweet = msg.value
    timestamp = datetime.strptime(tweet['timestamp'], '%Y-%m-%d %H:%M:%S')

    # Count fake tweets
    if tweet['label'].lower() == 'fake':
        fake_count += 1

    tweet_count += 1

    # Stop after a certain number of tweets
    if tweet_count >= 1000:  # You can adjust this to control the number of tweets
        break

end_time = datetime.now()

# Calculate and print time taken for processing
time_taken = (end_time - start_time).total_seconds()
print(f"⏱️ Time taken for streaming analysis: {time_taken} seconds")
print(f"⚠️ Fake tweets detected: {fake_count}")

