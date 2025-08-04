import urllib3
import json
from urllib.parse import urlencode

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Databricks workspace URL and personal access token
workspace_url = "https://your-databricks-workspace.cloud.databricks.com"
token = "your-personal-access-token"

# Create a connection pool
http = urllib3.PoolManager()

# Headers for the API requests
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json",
}

# Job configuration
job_config = {
    "name": "My Databricks Job",
    "existing_cluster_id": "your-cluster-id",
    "notebook_task": {
        "notebook_path": "/path/to/your/notebook",
    },
    "run_as_owner": True,
    "settings": {
        "job_description": "This is a sample job description",
        "parameters": [
            {"name": "param1", "value": "value1", "metadata": {"text": "Parameter 1 description"}},
            {"name": "param2", "value": "value2", "metadata": {"text": "Parameter 2 description"}},
            {"name": "param3", "value": "value3", "metadata": {"text": "Parameter 3 description"}},
        ]
    }
}

def get_job_id_by_name(job_name):
    list_jobs_url = f"{workspace_url}/api/2.1/jobs/list"
    response = http.request("GET", list_jobs_url, headers=headers)
    if response.status == 200:
        jobs = json.loads(response.data.decode('utf-8'))['jobs']
        for job in jobs:
            if job['settings']['name'] == job_name:
                return job['job_id']
    return None

def create_or_update_job(job_config):
    job_id = get_job_id_by_name(job_config['name'])
    
    if job_id:
        # Update existing job
        update_job_url = f"{workspace_url}/api/2.1/jobs/update"
        job_config['job_id'] = job_id
        response = http.request(
            "POST",
            update_job_url,
            body=json.dumps(job_config),
            headers=headers
        )
        action = "updated"
    else:
        # Create new job
        create_job_url = f"{workspace_url}/api/2.1/jobs/create"
        response = http.request(
            "POST",
            create_job_url,
            body=json.dumps(job_config),
            headers=headers
        )
        action = "created"

    if response.status == 200:
        job_id = json.loads(response.data.decode('utf-8')).get('job_id')
        print(f"Job {action} successfully. Job ID: {job_id}")
        return job_id
    else:
        print(f"Failed to {action} job. Status code: {response.status}")
        print(f"Response: {response.data.decode('utf-8')}")
        return None

def set_job_permissions(job_id, owner, group_name):
    permissions_url = f"{workspace_url}/api/2.0/permissions/jobs/{job_id}"
    
    # Define the desired permissions
    desired_permissions = {
        "access_control_list": [
            {
                "user_name": owner,
                "permission_level": "IS_OWNER"
            },
            {
                "group_name": group_name,
                "permission_level": "CAN_MANAGE_RUN"
            }
        ]
    }
    
    # Set the permissions
    response = http.request(
        "PUT",
        permissions_url,
        body=json.dumps(desired_permissions),
        headers=headers
    )

    if response.status == 200:
        print(f"Permissions set successfully
