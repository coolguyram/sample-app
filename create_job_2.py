import json
import urllib3

# === CONFIGURATION ===
DATABRICKS_HOST = "https://<your-databricks-instance>"  # e.g. https://adb-12345678.0.azuredatabricks.net
TOKEN = "<your-personal-access-token>"

NOTEBOOK_PATH = "/Repos/Analytics/Reports/MyNotebook"
JOB_NAME = "Sample Job with Service Principal"
SERVICE_PRINCIPAL_NAME = "xvy"
GROUP_NAME = "data-engineers"
JOB_DESCRIPTION = "This job runs a notebook using service principal 'xvy' and allows 'data-engineers' to manage runs."
INPUT_PARAMETERS = [
    {"name": "env", "default": "dev", "type": "text"},
    {"name": "date", "default": "2025-08-04", "type": "text"}
]

# === HTTP CLIENT ===
http = urllib3.PoolManager()

def databricks_api(method, endpoint, data=None, version="2.2"):
    url = f"{DATABRICKS_HOST}/api/{version}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    encoded_data = json.dumps(data).encode("utf-8") if data else None
    response = http.request(method, url, body=encoded_data, headers=headers)
    if response.status not in [200, 201]:
        raise Exception(f"API call failed: {response.status} {response.data.decode()}")
    return json.loads(response.data.decode()) if response.data else {}

# === 1. GET SERVICE PRINCIPAL ID ===
def get_service_principal_id(sp_name):
    resp = databricks_api("GET", f"preview/scim/v2/ServicePrincipals?filter=displayName eq \"{sp_name}\"", version="2.0")
    resources = resp.get("Resources", [])
    if not resources:
        raise Exception(f"Service Principal '{sp_name}' not found.")
    return resources[0]["id"]

# === 2. GET GROUP ID ===
def get_group_id(group_name):
    resp = databricks_api("GET", f"preview/scim/v2/Groups?filter=displayName eq \"{group_name}\"", version="2.0")
    resources = resp.get("Resources", [])
    if not resources:
        raise Exception(f"Group '{group_name}' not found.")
    return resources[0]["id"]

# === 3. FIND EXISTING JOB BY NOTEBOOK PATH ===
def find_job_by_notebook(notebook_path):
    resp = databricks_api("GET", "jobs/list")
    for job in resp.get("jobs", []):
        for task in job.get("settings", {}).get("tasks", []):
            if task.get("notebook_task", {}).get("notebook_path") == notebook_path:
                return job["job_id"]
    return None

# === 4. CREATE JOB ===
def create_job():
    job_data = {
        "name": JOB_NAME,
        "description": JOB_DESCRIPTION,
        "run_as": {
            "service_principal_name": SERVICE_PRINCIPAL_NAME
        },
        "tasks": [
            {
                "task_key": "notebook_task_1",
                "notebook_task": {
                    "notebook_path": NOTEBOOK_PATH,
                    "base_parameters": {p["name"]: p["default"] for p in INPUT_PARAMETERS}
                },
                "job_cluster_key": "default_cluster"
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "default_cluster",
                "new_cluster": {
                    "spark_version": "13.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2
                }
            }
        ],
        "parameters": [
            {
                "name": p["name"],
                "default": p["default"],
                "type": "text"
            } for p in INPUT_PARAMETERS
        ]
    }
    return databricks_api("POST", "jobs/create", job_data)

# === 5. UPDATE EXISTING JOB ===
def update_job(job_id):
    job_data = {
        "job_id": job_id,
        "new_settings": {
            "name": JOB_NAME,
            "description": JOB_DESCRIPTION,
            "run_as": {
                "service_principal_name": SERVICE_PRINCIPAL_NAME
            },
            "tasks": [
                {
                    "task_key": "notebook_task_1",
                    "notebook_task": {
                        "notebook_path": NOTEBOOK_PATH,
                        "base_parameters": {p["name"]: p["default"] for p in INPUT_PARAMETERS}
                    },
                    "job_cluster_key": "default_cluster"
                }
            ],
            "job_clusters": [
                {
                    "job_cluster_key": "default_cluster",
                    "new_cluster": {
                        "spark_version": "13.3.x-scala2.12",
                        "node_type_id": "i3.xlarge",
                        "num_workers": 2
                    }
                }
            ],
            "parameters": [
                {
                    "name": p["name"],
                    "default": p["default"],
                    "type": "text"
                } for p in INPUT_PARAMETERS
            ]
        }
    }
    return databricks_api("POST", "jobs/update", job_data)

# === 6. ASSIGN JOB PERMISSIONS ===
def set_job_permissions(job_id):
    permissions_data = {
        "access_control_list": [
            {
                "group_name": GROUP_NAME,
                "permission_level": "CAN_MANAGE_RUN"
            },
            {
                "service_principal_name": SERVICE_PRINCIPAL_NAME,
                "permission_level": "IS_OWNER"
            }
        ]
    }
    # Permissions API is still v2.0
    databricks_api("PATCH", f"permissions/jobs/{job_id}", permissions_data, version="2.0")

# === MAIN EXECUTION ===
if __name__ == "__main__":
    sp_id = get_service_principal_id(SERVICE_PRINCIPAL_NAME)
    group_id = get_group_id(GROUP_NAME)

    existing_job_id = find_job_by_notebook(NOTEBOOK_PATH)

    if existing_job_id:
        print(f"Job already exists (ID: {existing_job_id}), updating it...")
        update_job(existing_job_id)
        job_id = existing_job_id
    else:
        print("Job does not exist, creating a new one...")
        job = create_job()
        job_id = job["job_id"]

    set_job_permissions(job_id)
    print(f"Job configured with ID: {job_id}")
    print(f"Permissions updated for group '{GROUP_NAME}' and service principal '{SERVICE_PRINCIPAL_NAME}'.")
