import time
import json
import boto3
from prefect import flow, task, get_run_logger

LAMBDA_NAME = "aws-lakehouse-lambda-ingest"
GLUE_BRONZE_TO_SILVER = "aws-lakehouse-bronze-to-silver-orders"
GLUE_SILVER_TO_GOLD = "aws-lakehouse-silver-to-gold-orders"


@task(retries=2, retry_delay_seconds=30)
def trigger_lambda(function_name: str, payload: dict | None = None):
    logger = get_run_logger()
    client = boto3.client("lambda")

    resp = client.invoke(
        FunctionName=function_name,
        InvocationType="Event",  # async
        Payload=json.dumps(payload or {}).encode("utf-8"),
    )

    logger.info(f"Triggered Lambda: {function_name} | StatusCode={resp.get('StatusCode')}")
    return resp.get("StatusCode")


@task(retries=1, retry_delay_seconds=30)
def start_glue_job(job_name: str, arguments: dict | None = None) -> str:
    logger = get_run_logger()
    glue = boto3.client("glue")

    resp = glue.start_job_run(
        JobName=job_name,
        Arguments=arguments or {}
    )

    run_id = resp["JobRunId"]
    logger.info(f"Started Glue job: {job_name} | JobRunId={run_id}")
    return run_id


@task
def wait_for_glue_job(job_name: str, run_id: str, poll_seconds: int = 20) -> str:
    logger = get_run_logger()
    glue = boto3.client("glue")

    while True:
        resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = resp["JobRun"]["JobRunState"]
        logger.info(f"Glue job {job_name} ({run_id}) state: {state}")

        if state in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            return state

        time.sleep(poll_seconds)


@flow(name="aws-lakehouse-prefect-orchestration")
def lakehouse_flow():
    logger = get_run_logger()
    logger.info("Starting AWS Lakehouse orchestration (Prefect OSS local)")

    # 1) Ingest
    trigger_lambda(LAMBDA_NAME)

    # 2) Bronze -> Silver
    run1 = start_glue_job(GLUE_BRONZE_TO_SILVER)
    state1 = wait_for_glue_job(GLUE_BRONZE_TO_SILVER, run1)
    if state1 != "SUCCEEDED":
        raise RuntimeError(f"{GLUE_BRONZE_TO_SILVER} ended with state={state1}")

    # 3) Silver -> Gold
    run2 = start_glue_job(GLUE_SILVER_TO_GOLD)
    state2 = wait_for_glue_job(GLUE_SILVER_TO_GOLD, run2)
    if state2 != "SUCCEEDED":
        raise RuntimeError(f"{GLUE_SILVER_TO_GOLD} ended with state={state2}")

    logger.info("Pipeline finished successfully ✅")


if __name__ == "__main__":
    lakehouse_flow()
