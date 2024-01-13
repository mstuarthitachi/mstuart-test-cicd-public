import argparse
from pathlib import Path
import shutil

import api_client

BASE_REPO_FOLDER = Path("/Users/mstuart@hitachisolutions.com/mstuart_deployed")
OPS_NOTEBOOK_PATH = (
    BASE_REPO_FOLDER / "mstuart-test-cicd-public" / "example_ml_project" / "notebooks" / "operations" / "operations"
)
JOB_NAME = "cicd_run_operations"

email_notifications = {
        "on_success": [
            "mstuart@hitachisolutions.com"
        ],
        "on_failure": [
            "mstuart@hitachisolutions.com"
        ],
        "no_alert_for_skipped_runs": True,
    }

parser = argparse.ArgumentParser()
parser.add_argument("--client-id", required=True)
parser.add_argument("--client-secret", required=True)
parser.add_argument("--tenant-id", required=True)
parser.add_argument("--databricks-url", required=True)
parser.add_argument("--environment", required=True)
args = parser.parse_args()

# argparse changes dashes to underscores
print(f"The value of client-id is {args.client_id}")
print(f"The value of client-secret is {args.client_secret}")
print(f"The value of tenant-id is {args.tenant_id}")
print(f"The value of databricks-url is {args.databricks_url}")
print(f"The value of environment is {args.environment}")

environment=args.environment

client = api_client.DatabricksAPIClient(
    client_id=args.client_id,
    client_secret=args.client_secret,
    tenant_id=args.tenant_id,
    databricks_url=args.databricks_url

)

repos = [
    path.parent
    for path in Path(".").glob("*/notebooks")
    if path.glob("**/*.py") or path.glob("**/*.sql")
]
print(
    "Found these repos with a notebooks folder that has at least one .py or .sql file in it:",
    [str(repo) for repo in repos],
)

print("MSTUART")
print(f"The value of BASE_REPO_FOLDER is {BASE_REPO_FOLDER}")
print(f"The value of OPS_NOTEBOOK_PATH is {OPS_NOTEBOOK_PATH}")
print(f"The value of path.parent is {path.parent}")


print("Deleting notebooks in the destination workspace")
client.delete_remote_folder(folder_path=BASE_REPO_FOLDER)

print("Creating the base folder")
client.post("/api/2.0/workspace/mkdirs", {"path": str(BASE_REPO_FOLDER)})

print("Creating a warning for people")
client.post(
    "/api/2.0/workspace/mkdirs", {"path": str(BASE_REPO_FOLDER / "DONT_TOUCH_ANYTHING")}
)

for repo_root in repos:
    remote_dirname = BASE_REPO_FOLDER / repo_root

    print(f"Creating the repo folder for {repo_root}")
    client.post(
        "/api/2.0/workspace/mkdirs", {"path": str(BASE_REPO_FOLDER / repo_root)}
    )

    print(f"Zipping notebooks for {repo_root}")
    shutil.make_archive(
        base_name=repo_root / "notebooks",
        root_dir=repo_root,
        base_dir="notebooks",
        format="zip",
    )

    print(f"Uploading {repo_root} notebooks to the destination workspace")
    client.upload_resources(
        local_path=repo_root / "notebooks.zip", remote_path=remote_dirname / "notebooks"
    )

print("Kicking off operations job run")
job_def = {
    "name": JOB_NAME,
    "email_notifications": email_notifications,
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": JOB_NAME,
            "notebook_task": {
                "notebook_path": str(OPS_NOTEBOOK_PATH),
                "base_parameters": {
                    "ENVIRONMENT": environment
                },
                "source": "WORKSPACE"
            },
            "job_cluster_key": "Job_cluster",
            "timeout_seconds": 0,
            "email_notifications": {}
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "Job_cluster",
            "new_cluster": {
                "spark_version": "12.2.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "ON_DEMAND_AZURE",
                    "spot_bid_max_price": -1
                },
                "node_type_id": "Standard_DS3_v2",
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "enable_elastic_disk": True,
                "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
                "runtime_engine": "STANDARD",
                "num_workers": 0
            }
        }
    ],
    "format": "MULTI_TASK"
}

job_id = client.create_or_update_job(job_def)
run_def = {"job_id": job_id}
res = client.start_job_run(run_def)

# Build the link to the job
csrf_token = args.databricks_url.split("-")[1].split(".")[0]
link = f"{args.databricks_url}/?o={csrf_token}#job/{job_id}/run/{res['run_id']}"
print("Done. The Databricks job run should be here:")
print(link)
print(f"::notice title=Databricks Link::{link}")
