import boto3
import time
import argparse
from utils.logger import get_logger

logger = get_logger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
REGION          = "ap-south-1"
BUCKET          = "omini-financial-datalake"
SCRIPT_S3_PATH  = f"s3://{BUCKET}/scripts/silver_job.py"
LOG_S3_PATH     = f"s3://{BUCKET}/logs/emr/"

EMR_SERVICE_ROLE    = "omini-emr-service-role"
EMR_EC2_PROFILE     = "omini-emr-ec2-profile"

CLUSTER_NAME        = "omini-silver-cluster"
EMR_RELEASE         = "emr-7.12.0"


# ── Step 1: Upload PySpark script to S3 ──────────────────────────────────────
def upload_script(mode: str):
    """
    Always upload latest script from repo to S3 before launching EMR.
    This ensures EMR always runs the latest code from GitHub.
    """
    logger.info("Uploading silver_job.py to S3...")

    s3 = boto3.client("s3", region_name=REGION)
    s3.upload_file(
        Filename="scripts/silver_job.py",
        Bucket=BUCKET,
        Key="scripts/silver_job.py"
    )

    logger.info("Script uploaded → %s", SCRIPT_S3_PATH)


# ── Step 2: Launch EMR Cluster + Submit Job ───────────────────────────────────
def launch_emr(mode: str) -> str:
    """
    Spin up EMR cluster and submit silver_job.py as a step.
    Cluster auto-terminates after step completes.
    Returns cluster_id for monitoring.
    """
    logger.info("Launching EMR cluster in %s mode...", mode)

    emr = boto3.client("emr", region_name=REGION)

    response = emr.run_job_flow(
        Name=CLUSTER_NAME,
        ReleaseLabel=EMR_RELEASE,
        LogUri=LOG_S3_PATH,

        # ── Applications ─────────────────────────────────────────────────────
        Applications=[
            {"Name": "Spark"}
        ],

        # ── Instances ────────────────────────────────────────────────────────
        Instances={
            "InstanceGroups": [
                {
                    "Name":          "Primary",
                    "InstanceRole":  "MASTER",
                    "InstanceType":  "m5.xlarge",
                    "InstanceCount": 1,
                    "Market":        "ON_DEMAND",  # Primary always on-demand
                },
                {
                    "Name":          "Core",
                    "InstanceRole":  "CORE",
                    "InstanceType":  "m5.xlarge",
                    "InstanceCount": 1,
                    "Market":        "SPOT",        # Core on spot to save cost
                },
            ],
            "KeepJobFlowAliveWhenNoSteps": False,  # Auto-terminate when done
            "TerminationProtected":        False,
        },

        # ── PySpark Step ──────────────────────────────────────────────────────
        Steps=[
            {
                "Name": f"silver-job-{mode}",
                "ActionOnFailure": "TERMINATE_CLUSTER",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode", "cluster",
                        SCRIPT_S3_PATH,
                        "--mode", mode,
                    ],
                },
            }
        ],

        # ── IAM Roles ─────────────────────────────────────────────────────────
        JobFlowRole=EMR_EC2_PROFILE,
        ServiceRole=EMR_SERVICE_ROLE,

        # ── Auto Terminate ────────────────────────────────────────────────────
        AutoTerminationPolicy={"IdleTimeout": 3600},  # 1 hour safety net

        # ── Tags ──────────────────────────────────────────────────────────────
        Tags=[
            {"Key": "project", "Value": "omini-datalake"},
            {"Key": "mode",    "Value": mode},
        ],
    )

    cluster_id = response["JobFlowId"]
    logger.info("EMR cluster launched → %s", cluster_id)
    return cluster_id


# ── Step 3: Wait for Completion ───────────────────────────────────────────────
def wait_for_completion(cluster_id: str):
    """
    Poll EMR cluster status every 60 seconds.
    Log progress and exit when cluster terminates.
    """
    emr = boto3.client("emr", region_name=REGION)

    logger.info("Waiting for cluster %s to complete...", cluster_id)

    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state    = response["Cluster"]["Status"]["State"]
        reason   = response["Cluster"]["Status"].get("StateChangeReason", {}).get("Message", "")

        logger.info("Cluster status: %s", state)

        if state == "TERMINATED":
            logger.info("Cluster completed successfully!")
            break

        elif state == "TERMINATED_WITH_ERRORS":
            logger.error("Cluster failed! Reason: %s", reason)
            logger.error("Check logs at: %s%s/", LOG_S3_PATH, cluster_id)
            raise RuntimeError(f"EMR cluster failed: {reason}")

        elif state in ("TERMINATING", "WAITING", "RUNNING", "BOOTSTRAPPING", "STARTING"):
            time.sleep(60)  # check every 60 seconds

        else:
            logger.warning("Unknown state: %s", state)
            time.sleep(60)


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Omini EMR Launcher")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill"],
        default="daily",
        help="daily = today only | backfill = all history"
    )
    args = parser.parse_args()

    logger.info("========================================")
    logger.info("EMR Launcher — Mode: %s", args.mode)
    logger.info("========================================")

    upload_script(args.mode)
    cluster_id = launch_emr(args.mode)
    wait_for_completion(cluster_id)

    logger.info("========================================")
    logger.info("Silver layer complete!")
    logger.info("========================================")


if __name__ == "__main__":
    main()