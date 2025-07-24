from kafka import KafkaConsumer
import json
import mysql.connector

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="spark",
    password="rashSQL#1",  # replace with your actual password
    database="misinformation_db"
)
cursor = conn.cursor()

# Create table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS fake_tweets (
        id INT,
        username VARCHAR(255),
        content TEXT,
        matched_keywords TEXT,
        created_at DATETIME
    )
''')

# Kafka consumer setup
consumer = KafkaConsumer(
    'fake-tweets-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for fake tweets...")

for message in consumer:
    tweet = message.value

    # Use .get() to avoid KeyError
    tweet_id = tweet.get('id')
    username = tweet.get('username', 'unknown_user')
    content = tweet.get('content', 'No content')
    matched_keywords = json.dumps(tweet.get('matched_keywords', []))  # save as JSON string
    created_at = tweet.get('created_at')

    print(f"[FAKE TWEET] {created_at} - @{username}: {content}")

    # Insert into database
    cursor.execute('''
        INSERT INTO fake_tweets (id, username, content, matched_keywords, created_at)
        VALUES (%s, %s, %s, %s, %s)
    ''', (tweet_id, username, content, matched_keywords, created_at))

    conn.commit()

