import os
import boto3
import subprocess
import time
import logging
import datetime  # <-- Missing import fixed

# === Config ===
SYNC_DIR = r"sample"
S3_BUCKET = r"s3://authenta-test-aws-sync"
SQS_QUEUE_URL = r"https://sqs.us-east-1.amazonaws.com/832392893728/authenta-test-aws-sync"
REGION = "us-east-1"

# === Logging Setup ===
logging.basicConfig(
    filename='sync_logs.txt',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

def within_sync_hours():
    now = datetime.datetime.now()
    return 9 <= now.hour < 20  # 9:00 to 19:59

def sync_files():
    try:
        subprocess.run(
            ["aws", "s3", "sync", S3_BUCKET, SYNC_DIR, "--exact-timestamps", "--delete"],
            check=True
        )
        logging.info("✅ Sync successful.")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Sync failed: {e}")

def sqs_polling():
    logging.info("📡 Starting SQS polling...")
    
    try:
        sqs_client = boto3.client('sqs', region_name=REGION)
        logging.info("✅ SQS client initialized.")
    except Exception as e:
        logging.error(f"❌ SQS connection failed: {e}")
        return

    while True:
        try:
            if not within_sync_hours():
                print("⏰ Outside working hours. Sleeping for 5 minutes...")
                time.sleep(300)
                continue

            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20  # Max allowed by AWS
            )

            messages = response.get("Messages", [])

            if messages:
                for message in messages:
                    body = message.get('Body', '')
                    logging.info(f"📨 Received message: {body}")

                    if body.strip().upper() == "SYNC_TRIGGER":
                        sync_files()

                    try:
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        logging.info("🗑️ Message deleted.")
                    except Exception as e:
                        logging.error(f"❌ Failed to delete message: {e}")
            else:
                print("⏳ No messages. Sleeping 40s...")
                time.sleep(40)

        except KeyboardInterrupt:
            print("🛑 Interrupted by user.")
            logging.info("Stopped by user.")
            break
        except Exception as e:
            logging.error(f"Unhandled error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    if not all([SQS_QUEUE_URL, S3_BUCKET, SYNC_DIR]):
        print("🚫 Configuration error: Missing required settings.")
        exit(1)

    sqs_polling()
