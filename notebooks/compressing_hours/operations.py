# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC This notebook is one of the entry points for the Data Science (DS) team. This operations notebook is specific to the single model project. It is triggered by the highest level operations notebook. This notebook **continues** to set up the databricks workspace by:
# MAGIC
# MAGIC 1. Creating delta lake schema and tables **that the DS team owns**
# MAGIC 2. Creating mlflow models/environments
# MAGIC 3. Creating jobs - including job permissions - **that the DS team owns**

# COMMAND ----------

# DBTITLE 1,Dev Variables
# dbutils.widgets.removeAll()
# dbutils.widgets.text("ENVIRONMENT", "dev")

# COMMAND ----------

# DBTITLE 1,Environment Variables
# Get widgets
environment = dbutils.widgets.get("ENVIRONMENT").lower()

# COMMAND ----------

# MAGIC %md # Create Jobs
# MAGIC This is the easiest way to generate the code below:
# MAGIC 1. Create the job via point-and-click in the Databricks UI (include all settings you're interested in).
# MAGIC 2. "View JSON" for the job and copy the JSON. You only want the JSON within the "settings" key.
# MAGIC 3. Paste the JSON in this operations notebook and replace some of the values with variables (like notebook path, table names, etc.)

# COMMAND ----------

# MAGIC %run ../utilities/databricks_rest_api

# COMMAND ----------

# dbutils.secrets.listScopes()

# COMMAND ----------

# dbutils.secrets.list("key-vault-secrets")

# COMMAND ----------

# DBTITLE 1,Need to set up permissions when creating the jobs
# Set permisisons to give data science team the highest permissions in dev and
# lower permissions in all other environments
service_principal_id = get_service_principal_id()

if environment == 'dev':
    job_permissions = {
        'access_control_list': [
            {'service_principal_name': service_principal_id, 'permission_level': 'IS_OWNER'},
            {'group_name': 'data_science_team', 'permission_level': 'CAN_MANAGE'}
        ]
    }
else:
    job_permissions = {
        'access_control_list': [
            {'service_principal_name': service_principal_id, 'permission_level': 'IS_OWNER'},
            {'group_name': 'data_science_team', 'permission_level': 'CAN_MANAGE_RUN'}
        ]
    } 

print(service_principal_id)
print(environment)
print(job_permissions)

# COMMAND ----------

email_notifications = {
        "on_success": [
            "mstuart@hitachisolutions.com"
        ],
        "on_failure": [
            "mstuart@hitachisolutions.com"
        ],
        "no_alert_for_skipped_runs": True,
    }
    
print(email_notifications)

# COMMAND ----------

job_def_compressing_hours_etl = {
  "name": "mstuart_test_create_job",
  "email_notifications": email_notifications,
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "mstuart_create_job",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Repos/mstuart@hitachisolutions.com/mstuart-test-cicd-public/notebooks/compressing_hours/etl/etl_do_nothing",
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
        "spark_version": "13.3.x-scala2.12",
        "spark_conf": {
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
        "spark_env_vars": {
          "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": True,
        "policy_id": "D063CB9F3B0008F4",
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 0
      }
    }
  ],
}

# COMMAND ----------

job_def_compressing_hours_etl_id = create_or_update_job(job_def_compressing_hours_etl)
print(job_def_compressing_hours_etl_id)

replace_permissions(job_def_compressing_hours_etl_id, job_permissions)

# COMMAND ----------

start_run(job_def_compressing_hours_etl_id)
