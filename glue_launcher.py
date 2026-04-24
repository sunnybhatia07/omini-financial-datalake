import boto3
from utils.logger import get_logger

logger = get_logger(__name__)

REGION   = "ap-south-1"
JOB_NAME = "omini-silver-daily"


def main():
    glue = boto3.client("glue", region_name=REGION)

    logger.info("Triggering Glue job: %s", JOB_NAME)

    response = glue.start_job_run(JobName=JOB_NAME)
    run_id = response["JobRunId"]

    logger.info("Glue job started → %s", run_id)

    # Wait for completion
    while True:
        status = glue.get_job_run(JobName=JOB_NAME, RunId=run_id)
        state  = status["JobRun"]["JobRunState"]
        logger.info("Glue job state: %s", state)

        if state == "SUCCEEDED":
            logger.info("Glue job completed successfully!")
            break
        elif state in ("FAILED", "ERROR", "TIMEOUT"):
            error = status["JobRun"].get("ErrorMessage", "")
            logger.error("Glue job failed: %s", error)
            raise RuntimeError(f"Glue job failed: {error}")

        import time
        time.sleep(30)


if __name__ == "__main__":
    main()