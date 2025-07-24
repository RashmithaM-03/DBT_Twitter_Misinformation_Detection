import pandas as pd
from sqlalchemy import create_engine, text

# Configuration - Update these with your credentials
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "spark",
    "password": "rashSQL#1",
    "database": "misinformation_db"
}

FAKE_KEYWORDS = [
    "scam", "fake", "hoax", "misleading", "false",
    "conspiracy", "rumor", "unverified", "clickbait",
    "deceive", "fabricated", "manipulated", "fraud"
]

BATCH_SIZE = 1000  # Set the batch size for processing

def analyze_tweets(engine):
    try:
        # 1. Load tweets in batches
        with engine.connect() as conn:
            total_count = 0
            fake_count = 0

            # Initialize offset for batch processing
            offset = 0
            
            while True:
                # Fetch tweets in batches
                tweets_df = pd.read_sql(
                    text(f"SELECT id, tweet as text, username, created_at FROM tweets LIMIT {BATCH_SIZE} OFFSET {offset}"),
                    conn
                )

                if tweets_df.empty:
                    break  # Exit if no more tweets are left

                total_count += len(tweets_df)
                
                # 2. Detect fake tweets using keywords
                tweets_df['is_fake'] = tweets_df['text'].str.lower().str.contains(
                    '|'.join(FAKE_KEYWORDS), na=False
                )
                fake_count += tweets_df['is_fake'].sum()

                # 3. Process and report every batch
                print(f"\n=== BATCH ANALYSIS REPORT ===")
                print(f"Total Tweets Analyzed so far: {total_count}")
                print(f"Potential Fake Tweets Detected so far: {fake_count} ({fake_count/total_count:.1%})")
                
                # 4. Show keyword statistics for the batch
                keyword_stats = []
                for kw in FAKE_KEYWORDS:
                    count = tweets_df['text'].str.lower().str.contains(kw, na=False).sum()
                    if count > 0:
                        keyword_stats.append((kw, count))
                
                print(pd.DataFrame(keyword_stats, columns=['Keyword', 'Matches']).sort_values('Matches', ascending=False))

                # 5. Show top fake tweets for this batch
                fake_tweets = tweets_df[tweets_df['is_fake']].sort_values(
                    'created_at', ascending=False
                ).head(10)

                if not fake_tweets.empty:
                    print("\n=== TOP FAKE TWEETS ===")
                    for idx, row in fake_tweets.iterrows():
                        print(f"\n[{row['created_at']}] @{row['username']}")
                        print("Matched keywords:", [kw for kw in FAKE_KEYWORDS if kw in str(row['text']).lower()])
                        print(f"{row['text']}\n{'='*50}")

                # 6. Update offset for next batch
                offset += BATCH_SIZE

            # Final summary
            print(f"\n=== FINAL FAKE TWEET ANALYSIS REPORT ===")
            print(f"Total Tweets Analyzed: {total_count}")
            print(f"Potential Fake Tweets Detected: {fake_count} ({fake_count/total_count:.1%})")

    except Exception as e:
        print(f"\nError during analysis: {str(e)}")

def main():
    # Create database connection
    engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}"
    )
    
    analyze_tweets(engine)
    engine.dispose()

if __name__ == "__main__":
    main()

