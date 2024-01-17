# Databricks notebook source
# MAGIC %md The following functions are commonly used databricks rest api functions for interacting with service principal, changing permissions, etc. 

# COMMAND ----------

# DBTITLE 1,Imports
import requests

key_vault_scope = 'key-vault-secrets'

# Service Principal Secrets
# sp_service_key = 'databricks-sp-secret'
# sp_app_id_key = 'databricks-sp-app-id'
# sp_directory_id_key = 'databricks-sp-directory-id'

# MSTUART
sp_app_id_key = 'service-principal-app-id'
sp_service_key = 'service-principal-app-secret'
sp_directory_id_key = 'service-principal-tenant-id'

# COMMAND ----------

# DBTITLE 1,Initialize Variables
def get_service_principal_id():
    return dbutils.secrets.get(scope = key_vault_scope, key = sp_app_id_key)

def create_azure_ad_token_for_service_principle():
    tenant_id = dbutils.secrets.get(scope = key_vault_scope, key = sp_directory_id_key)
    application_id = dbutils.secrets.get(scope = key_vault_scope, key = sp_app_id_key)
    secret_value = dbutils.secrets.get(scope = key_vault_scope, key = sp_service_key)

    url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
    headers = {'Content-Type':'application/x-www-form-urlencoded'}
    payload = {
        "client_id": application_id,
        "grant_type": "client_credentials",
        "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
        "client_secret": secret_value
    }
    r = requests.post(url, headers=headers, data=payload)
    if r.status_code == 200:
        return r.json()['access_token']
    else:
        print(f"ERROR create_azure_ad_token_for_service_principle: {r.json()}")
        return r

databricks_root_url = 'https://' + spark.conf.get("spark.databricks.workspaceUrl")
databricks_token = create_azure_ad_token_for_service_principle()
headers = {'Content-Type':'application/json', 'Authorization': 'Bearer ' + databricks_token}

# COMMAND ----------

# DBTITLE 1,Permissions Functions
def replace_permissions(job_id: int, permissions: dict):
    '''
    This function replaces the permissions for an existing job
    '''
    url = f'{databricks_root_url}/api/2.0/permissions/jobs/{job_id}'
    r = requests.put(url, headers=headers, json=permissions)
    if r.status_code == 200:
        print(f"Updated permissions for job_id {job_id}")
    else:
        print(f"ERROR: replace permissions {r.json()}")
        

# COMMAND ----------

# DBTITLE 1,Workspace Functions
def create_workspace_dir(path: str):
    url = f'{databricks_root_url}/api/2.0/workspace/mkdirs'
    r = requests.post(url, headers=headers, json={"path": path})
    if r.status_code == 200:
        print(f"Created Workspace path {path} if it didn't already exist")
    else:
        print(f"ERROR: create_workspace_dir {r.json()}")
    

# COMMAND ----------

# DBTITLE 1,Get Current Jobs
def get_some_jobs(offset=0):
    url = databricks_root_url + '/api/2.1/jobs/list?offset=' + str(offset)
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"ERROR: get_some_jobs(offset={offset}) {r.json()}")

def get_current_jobs():
    job_ids = {}
    offset = 0
    jobs = get_some_jobs(offset).get("jobs", [])
    while jobs:
        for job in jobs:
            job_ids[job["settings"]["name"]] = job["job_id"]
        offset += len(jobs)
        jobs = get_some_jobs(offset).get("jobs", [])
    return job_ids

# COMMAND ----------

# DBTITLE 1,Get Existing Job Definition
def get_job(job_id):
    url = databricks_root_url + '/api/2.1/jobs/get'
    r = requests.get(url, headers=headers, json={"job_id": job_id})
    if r.status_code == 200:
        job = r.json()
        return job
    else:
        print(f"ERROR: create_or_update_job {r.json()}")

# COMMAND ----------

# DBTITLE 1,Create or Update Jobs
def create_or_update_job(job_definition):
    job_name = job_definition['name']
    existing_jobs = get_current_jobs()
    if type(existing_jobs) is dict:
        if job_name not in existing_jobs:
            # Create job
            url = databricks_root_url + '/api/2.1/jobs/create'
            r = requests.post(url, headers=headers, json=job_definition)
            if r.status_code == 200:
                job_id = r.json()['job_id']
                print('Created job', job_id, job_name)
                return job_id
            else:
                print(f"ERROR: create_or_update_job {r.json()}")
        else:
            # Reset existing job
            existing_job_id = existing_jobs[job_name]
            url = databricks_root_url + '/api/2.1/jobs/reset'
            r = requests.post(url, headers=headers, json={"job_id": existing_job_id, "new_settings": job_definition})
            if r.status_code == 200:
                print('Reset existing job', existing_job_id, job_name)
                return existing_job_id
            else:
                print(f"ERROR: create_or_update_job {r.json()}")
    else:
        # Create job
        url = databricks_root_url + '/api/2.1/jobs/create'
        r = requests.post(url, headers=headers, json=job_definition)
        if r.status_code == 200:
            job_id = r.json()['job_id']
            print('Created job', job_id, job_name)
            return job_id
        else:
            print(f"ERROR: create_or_update_job {r.json()}")

# COMMAND ----------

def start_run(job_id):
    url = databricks_root_url + '/api/2.1/jobs/run-now'
    r = requests.post(url, headers=headers, json={"job_id":job_id})
    if r.status_code != 200:
        print('ERROR: start_run: status code ' + str(r.status_code))
        print(r.json())
