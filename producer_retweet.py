from kafka import KafkaProducer
import json
import pandas as pd
import time

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the dataset
df = pd.read_csv('Constraint_Train_Modified.csv')  # Ensure this file has 'retweet_count' column

# Stream tweets
for _, row in df.iterrows():
    tweet = {
        'username': row['username'],
        'content': row['tweet'],
        'retweet_count': row['retweet_count'],  # ✅ Only using retweets
        'timestamp': row['created_at'],
        'label': row['label']
    }

    # Send to all 3 topics
    producer.send('fake-tweets-topic', tweet)
    producer.send('most-liked-users-topic', tweet)
    producer.send('top-fake-users-topic', tweet)

    # Display what's being sent
    print(f"📤 Sent tweet by @{tweet['username']} at {tweet['timestamp']}:")
    print(f"    \"{tweet['content']}\" | 🔁 Retweets: {tweet['retweet_count']}\n")

    time.sleep(0.5)  # Simulate delay

producer.flush()
print("✅ All tweets streamed successfully.")

