from kafka import KafkaConsumer
import json
import mysql.connector

# Kafka consumer setup
consumer = KafkaConsumer(
    'most-liked-users-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="spark",
    password="rashSQL#1",  # <-- replace this with your MySQL password
    database="misinformation_db"
)
cursor = db.cursor()

# Create table if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS top_liked_users (
        username VARCHAR(255),
        total_likes INT
    )
''')

print("Listening for top liked users...")

for msg in consumer:
    tweet = msg.value
    username = tweet['username']
    likes = tweet['likes']

    print(f"[LIKED USER] @{username}: {likes} likes")

    try:
        cursor.execute('''
            INSERT INTO top_liked_users (username, total_likes)
            VALUES (%s, %s)
        ''', (username, int(likes)))
        db.commit()
    except Exception as e:
        print(f"⚠️ Error saving entry: {e}")

