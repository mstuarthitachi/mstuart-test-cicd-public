from typing import Dict

import requests

class DatabricksAPIClient:
    """A lil' client to help with the Databricks API"""

    _token: str = None

    def __init__(
        self, client_id: str, client_secret: str, tenant_id: str, databricks_url: str
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.databricks_url = databricks_url

    def fail_on_error(self, res: requests.Response) -> None:
        try:
            res.raise_for_status()
        except requests.HTTPError as e:
            print(e.response.content)
            raise

    @property
    def token(self) -> str:
        if self._token:
            return self._token

        print("Getting token first")
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "client_id": self.client_id,
            "grant_type": "client_credentials",
            "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
            "client_secret": self.client_secret,
        }
        res = requests.post(url, headers=headers, data=payload)
        self.fail_on_error(res)

        self._token = res.json()["access_token"]
        return self._token

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + self.token,
        }

    def get(self, endpoint: str) -> dict:
        res = requests.get(self.databricks_url + endpoint, headers=self.headers)
        self.fail_on_error(res)
        return res.json()

    def post(self, endpoint: str, payload: dict) -> dict:
        res = requests.post(
            self.databricks_url + endpoint, headers=self.headers, json=payload
        )
        self.fail_on_error(res)
        return res.json()

    def upload_file(
        self, endpoint: str, local_filepath: str, other_payload: dict
    ) -> dict:
        with open(local_filepath, "rb") as f:
            res = requests.post(
                self.databricks_url + endpoint,
                headers={k: v for k, v in self.headers.items() if k != "Content-Type"},
                data=other_payload,
                files={"content": f},
            )
        self.fail_on_error(res)
        return res.json()

    # Supporting functions
    def delete_remote_folder(self, folder_path: str) -> None:
        print(f"Deleting {folder_path}")
        try:
            self.post(
                "/api/2.0/workspace/delete",
                {"path": str(folder_path), "recursive": True},
            )
        except requests.HTTPError as e:
            if e.response.json()["error_code"] == "RESOURCE_DOES_NOT_EXIST":
                print("Path did not exist, continuing")
            else:
                raise

    def upload_resources(self, local_path: str, remote_path: str) -> None:
        print(f"Uploading {local_path} to {remote_path}")
        self.upload_file(
            "/api/2.0/workspace/import", local_path, {"path": str(remote_path)}
        )

    def get_current_jobs(self) -> Dict[str, int]:
        url = "/api/2.1/jobs/list?offset="
        job_ids = {}
        offset = 0
        jobs = self.get(url + str(offset)).get("jobs", [])

        while jobs:
            for job in jobs:
                job_ids[job["settings"]["name"]] = job["job_id"]
            offset += len(jobs)
            jobs = self.get(url + str(offset)).get("jobs", [])

        return job_ids

    def create_or_update_job(self, job_definition: dict) -> int:
        job_name = job_definition["name"]
        existing_jobs = self.get_current_jobs()
        try:
            job_id = existing_jobs[job_name]
            print("Updating old job")
            res = self.post(
                "/api/2.1/jobs/reset",
                {"job_id": job_id, "new_settings": job_definition},
            )
        except:
            print("Creating new job")
            res = self.post("/api/2.1/jobs/create", job_definition)
            job_id = res["job_id"]
        return job_id

    def start_job_run(self, run_definition: dict) -> dict:
        job_id = run_definition["job_id"]
        print("Starting run for job_id", job_id)
        res = self.post("/api/2.1/jobs/run-now", run_definition)
        return res
