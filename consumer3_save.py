from kafka import KafkaConsumer
import json
import mysql.connector

# Kafka consumer setup
consumer = KafkaConsumer(
    'top-fake-users-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="spark",
    password="rashSQL#1",
    database="misinformation_db"
)
cursor = db.cursor()

# Create a more descriptive table for top fake users by retweets
cursor.execute('''
    CREATE TABLE IF NOT EXISTS top_fake_retweet_users (
        username VARCHAR(255) PRIMARY KEY,
        total_fake_retweets INT
    )
''')

print("👂 Listening for top fake users (based on retweets)...")

# Track fake tweet retweet count per user
user_fake_retweets = {}
TOP_N = 5  # Limit to top 5 users

for msg in consumer:
    tweet = msg.value
    username = tweet.get('username')
    label = tweet.get('label')
    retweet_count = tweet.get('retweet_count', 0)

    # Consider only fake tweets
    if label and label.lower() == 'fake':
        try:
            retweet_count = int(retweet_count)
        except ValueError:
            retweet_count = 0

        user_fake_retweets[username] = user_fake_retweets.get(username, 0) + retweet_count
        print(f"[FAKE RETWEET USER] @{username}: {user_fake_retweets[username]} fake retweets")

        # Get top N users
        top_users = sorted(user_fake_retweets.items(), key=lambda x: x[1], reverse=True)[:TOP_N]

        try:
            # Clear and update top users
            cursor.execute("DELETE FROM top_fake_retweet_users")
            for user, total_retweets in top_users:
                cursor.execute('''
                    INSERT INTO top_fake_retweet_users (username, total_fake_retweets)
                    VALUES (%s, %s)
                ''', (user, total_retweets))
            db.commit()
        except Exception as e:
            print(f"⚠️ Error saving to DB: {e}")

