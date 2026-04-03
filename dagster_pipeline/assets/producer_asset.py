from dagster import asset
import subprocess


@asset
def run_producer():
    result = subprocess.run(["python","ingestion/producer.py"],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Producer script failed with error: {result.stderr}")
    return "Producer executed successfully"

