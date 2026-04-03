from dagster import asset
import subprocess

@asset
def run_consumer():
    consu_res = subprocess.run(["python","ingestion/consumer.py"],
                               capture_output=True, text=True)

    if consu_res.returncode != 0:
        raise Exception(f"Consumer script failed with error: {consu_res.stderr}")
    return "Consumer executed successfully"

