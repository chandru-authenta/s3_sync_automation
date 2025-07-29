import os
import boto3
import subprocess
import time
import logging

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

def sync_files():
    # print("🔄 Syncing from S3 to local folder...")
    try:
        subprocess.run(
            ["aws", "s3", "sync", S3_BUCKET, SYNC_DIR, "--exact-timestamps","--delete"],
            check=True
        )
        # print("✅ Sync completed.")
        logging.info("Sync successful.")
    except subprocess.CalledProcessError as e:
        # print(f"❌ Sync failed: {e}")
        logging.error(f"Sync failed: {e}")

def sqs_polling():
    # print("📡 Starting SQS polling...")
    
    # SQS client setup
    try:
        sqs_client = boto3.client('sqs', region_name=REGION)
        # print("✅ SQS connection established.")
    except Exception as e:
        # print(f"❌ SQS connection failed: {e}")
        return

    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20  # Long polling
            )
        except Exception as e:
            logging.error(f"Error receiving SQS messages: {e}")
            time.sleep(10)
            continue

        messages = response.get("Messages", [])

        if messages:
            for message in messages:
                body = message.get('Body', '')
                # print(f"📨 Message received: {body}")
                logging.info(f"Received SQS message: {body}")

                if body.strip().upper() == "SYNC_TRIGGER":
                    sync_files()

                # Delete message
                try:
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    logging.info("SQS message deleted.")
                except Exception as e:
                    logging.error(f"Failed to delete SQS message: {e}")
        else:
            print("⏳ No messages. Waiting...")
        
        time.sleep(40)

if __name__ == "__main__":
    # Validate variables
    if not SQS_QUEUE_URL or not S3_BUCKET or not SYNC_DIR:
        print("🚫 Configuration error: Check SQS_QUEUE_URL, S3_BUCKET, SYNC_DIR.")
        exit(1)

    # Start polling
    sqs_polling()
