from pathlib import Path
import subprocess
import sys
import time

from dagster import asset


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@asset
def run_ingestion_cycle(context):
    consumer_command = [
        sys.executable,
        "ingestion/consumer.py",
        "--startup-timeout",
        "30",
        "--idle-timeout",
        "5",
    ]
    producer_command = [sys.executable, "ingestion/producer.py"]

    context.log.info("Starting Kafka consumer for this ingestion cycle")
    consumer_process = subprocess.Popen(
        consumer_command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give the consumer a moment to subscribe before the producer publishes.
        time.sleep(2)
        context.log.info("Starting Kafka producer for this ingestion cycle")
        producer_result = subprocess.run(
            producer_command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
        )

        if producer_result.stdout:
            context.log.info(f"Producer output:\n{producer_result.stdout.strip()}")
        if producer_result.stderr:
            context.log.warning(f"Producer stderr:\n{producer_result.stderr.strip()}")

        if producer_result.returncode != 0:
            consumer_process.terminate()
            raise Exception(f"Producer script failed with error: {producer_result.stderr}")

        try:
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=20)
        except subprocess.TimeoutExpired as exc:
            consumer_process.terminate()
            consumer_stdout, consumer_stderr = consumer_process.communicate(timeout=10)
            raise Exception(
                "Consumer did not drain Kafka and exit within the expected time window."
            ) from exc

        if consumer_stdout:
            context.log.info(f"Consumer output:\n{consumer_stdout.strip()}")
        if consumer_stderr:
            context.log.warning(f"Consumer stderr:\n{consumer_stderr.strip()}")

        if consumer_process.returncode != 0:
            raise Exception(f"Consumer script failed with error: {consumer_stderr}")

        return "Producer and consumer completed one bounded ingestion cycle successfully"
    finally:
        if consumer_process.poll() is None:
            consumer_process.terminate()
