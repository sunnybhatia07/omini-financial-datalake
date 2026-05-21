import json
import boto3
from utils.logger import get_logger
from ingestion.ingestion_main import main as run_ingestion

logger = get_logger(__name__)

REGION   = "ap-south-1"
JOB_NAME = "omini-silver-daily"


def trigger_glue():
    """
    Trigger Glue silver job after bronze ingestion completes.
    Fire and forget — Lambda doesn't wait for Glue to finish.
    Glue runs independently after being triggered.
    """
    glue = boto3.client("glue", region_name=REGION)

    response = glue.start_job_run(JobName=JOB_NAME)
    run_id = response["JobRunId"]

    logger.info("Glue job triggered → %s", run_id)
    return run_id


def lambda_handler(event, context):
    """
    AWS Lambda entry point.

    Called by EventBridge at 8:30 PM IST daily.

    Flow:
    1. Run bronze ingestion (batch download → S3)
    2. Trigger Glue silver job
    3. Return success
    """
    logger.info("========================================")
    logger.info("Lambda handler started")
    logger.info("========================================")

    try:
        # Step 1 — Run bronze ingestion
        logger.info("Starting bronze ingestion...")
        run_ingestion()
        logger.info("Bronze ingestion complete!")

        # Step 2 — Trigger Glue silver job
        logger.info("Triggering Glue silver job...")
        run_id = trigger_glue()
        logger.info("Glue triggered successfully → %s", run_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status":      "success",
                "glue_run_id": run_id
            })
        }

    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "failed",
                "error":  str(e)
            })
        }