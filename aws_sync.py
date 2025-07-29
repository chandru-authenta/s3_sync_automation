import time
import os
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from threading import Timer
import boto3

# === Config ===
WATCH_DIR = r"C:\Users\hise\Documents\testing\sample"  # Local folder to sync
S3_BUCKET = r"s3://authenta-test-aws-sync"
IDLE_TIME = 120  # Seconds after last activity before syncing

def send_sqs(message):
    try:
        conn=boto3.client('sqs',region_name="us-east-1")
        
        SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/832392893728/authenta-test-aws-sync"
        conn.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=message
        )
        # print("📤 SQS notification sent.")
    except Exception as e:
        print(f"❌ Failed to send SQS notification: {e}")
    



class SyncAfterIdleHandler:
    def __init__(self):
        self.timer = None

    def reset_timer(self):
        if self.timer:
            self.timer.cancel()
        self.timer = Timer(IDLE_TIME, self.run_sync)
        self.timer.start()

    def run_sync(self):
        # print("🟢 Idle for 120s. Preparing to sync with AWS S3...")

        # Step 1: Log what will be deleted
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file = f"deleted_log_{timestamp}.txt"

        # print(f"🔍 Logging deletions to {log_file}...")
        try:
            dryrun = subprocess.run(
                ["aws", "s3", "sync", WATCH_DIR, S3_BUCKET, "--delete", "--exact-timestamps", "--dryrun"],
                check=True,
                capture_output=True,
                text=True
            )
            deletions = [line for line in dryrun.stdout.splitlines() if "delete:" in line]
            if deletions:
                log_file = f"deleted_log_{timestamp}.txt"
                # print(f"🔍 Logging deletions to {log_file}...")
                with open(log_file, 'w') as f:
                    for line in deletions:
                        f.write(line + "\n")
            # else:
                # print("✅ No deletions detected. No deletion log created.")
        except subprocess.CalledProcessError as e:
            # print("❌ Dry-run failed:", e)
            return

        # Step 2: Perform actual sync
        # print("🚀 Syncing to S3 with deletion enabled...")
        try:
            sync = subprocess.run(
                ["aws", "s3", "sync", WATCH_DIR, S3_BUCKET, "--delete", "--exact-timestamps"],
                check=True,
                capture_output=True,
                text=True
            )
            send_sqs("SYNC_TRIGGER")
            with open("creation_logs.txt", 'a') as f:
                f.write(f"[{time.ctime()}] Sync completed.\n")
                f.write(sync.stdout + "\n")

            print("✅ Sync completed.")
        except subprocess.CalledProcessError as e:
            # print(f"❌ Sync failed: {e}")
            with open("creation_logs.txt", 'a') as f:
                f.write(f"[{time.ctime()}] Sync failed: {e}\n")
                f.write(sync.stderr)


class MyFileSystemEventHandler(FileSystemEventHandler):
    def __init__(self):
        self.sync_handler = SyncAfterIdleHandler()

    def _should_ignore(self, path):
        return os.path.basename(path).startswith('.')

    def on_created(self, event):
        if not self._should_ignore(event.src_path):
            # print(f"🟢 File created: {event.src_path}")
            self.sync_handler.reset_timer()

    def on_deleted(self, event):
        if not self._should_ignore(event.src_path):
            # print(f"🟠 File deleted: {event.src_path}")
            self.sync_handler.reset_timer()

    def on_modified(self, event):
        if not self._should_ignore(event.src_path):
            # print(f"🟡 File modified: {event.src_path}")
            self.sync_handler.reset_timer()


if __name__ == "__main__":
    # print(f"📁 Watching folder: {WATCH_DIR}...")
    event_handler = MyFileSystemEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=WATCH_DIR, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        # print("\n🛑 Stopping observer.")
        observer.stop()
    observer.join()
