import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "spark",
    "password": "rashSQL#1",
    "database": "misinformation_db"
}

# Set up time tracking
start_time = datetime.now()

def analyze_tweets_batch(engine):
    try:
        # Load all tweets from the database
        tweets_df = pd.read_sql(
            "SELECT id, tweet as text, username, created_at FROM tweets", 
            engine
        )

        # Detect fake tweets
        tweets_df['is_fake'] = tweets_df['text'].str.lower().str.contains('fake|misleading|scam', na=False)
        fake_count = tweets_df['is_fake'].sum()

        return fake_count

    except Exception as e:
        print(f"\nError during batch analysis: {str(e)}")
        return 0

# Create a database connection
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
    f"{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"
)

# Run batch analysis
fake_count = analyze_tweets_batch(engine)

# End time
end_time = datetime.now()

# Calculate time taken for batch processing
time_taken = (end_time - start_time).total_seconds()
print(f"⏱️ Time taken for batch analysis: {time_taken} seconds")
print(f"⚠️ Fake tweets detected: {fake_count}")

