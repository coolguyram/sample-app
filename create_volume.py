import urllib3
import json
import certifi

# Disable InsecureRequestWarning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Databricks workspace URL and personal access token
workspace_url = "https://your-databricks-workspace.cloud.databricks.com"
token = "your-personal-access-token"

# API endpoints
volumes_endpoint = f"{workspace_url}/api/2.0/volumes"

# Headers for the API request
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Create a PoolManager instance
http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

# Volume configuration
volume_config = {
    "name": "my_new_volume",
    "catalog_name": "my_catalog",
    "schema_name": "my_schema",
    "volume_type": "EXTERNAL",
    "storage_location": "s3://your-bucket/new-prefix-path/",
    "comment": "Volume created via REST API"
}

def get_volume_id(catalog, schema, name):
    response = http.request(
        'GET',
        f"{volumes_endpoint}?catalog_name={catalog}&schema_name={schema}",
        headers=headers
    )
    if response.status == 200:
        volumes = json.loads(response.data.decode('utf-8'))['volumes']
        for volume in volumes:
            if volume['name'] == name:
                return volume['volume_id']
    return None

def create_or_update_volume():
    volume_id = get_volume_id(volume_config['catalog_name'], volume_config['schema_name'], volume_config['name'])
    
    if volume_id:
        print(f"Volume '{volume_config['name']}' already exists. Updating...")
        update_endpoint = f"{volumes_endpoint}/{volume_id}"
        response = http.request(
            'PATCH',
            update_endpoint,
            body=json.dumps(volume_config).encode('utf-8'),
            headers=headers
        )
    else:
        print(f"Creating new volume '{volume_config['name']}'...")
        response = http.request(
            'POST',
            volumes_endpoint,
            body=json.dumps(volume_config).encode('utf-8'),
            headers=headers
        )

    if response.status in [200, 201]:
        volume_id = json.loads(response.data.decode('utf-8'))["volume_id"]
        print(f"Volume operation successful. Volume ID: {volume_id}")
        return volume_id
    else:
        print(f"Failed to create/update volume. Status code: {response.status}")
        print(f"Error message: {response.data.decode('utf-8')}")
        return None

def get_current_permissions(volume_id):
    permissions_endpoint = f"{volumes_endpoint}/{volume_id}/permissions"
    response = http.request('GET', permissions_endpoint, headers=headers)
    if response.status == 200:
        return json.loads(response.data.decode('utf-8'))
    return None

def update_permissions(volume_id):
    permissions_config = {
        "changes": [
            {
                "principal": "xyz",
                "add": ["READ", "WRITE"]
            },
            {
                "principal": "group A",
                "add": ["READ"]
            }
        ]
    }

    current_permissions = get_current_permissions(volume_id)
    if current_permissions is None:
        print(f"Failed to retrieve current permissions for volume ID: {volume_id}")
        return False

    changes_needed = False
    for change in permissions_config['changes']:
        principal = change['principal']
        required_permissions = set(change['add'])
        
        if principal in current_permissions['access_control_list']:
            existing_permissions = set(current_permissions['access_control_list'][principal]['permissions'])
            missing_permissions = required_permissions - existing_permissions
            if missing_permissions:
                change['add'] = list(missing_permissions)
                changes_needed = True
            else:
                permissions_config['changes'].remove(change)
        else:
            changes_needed = True

    if not changes_needed:
        print("No permission changes needed. All required permissions are already set.")
        return True

    permissions_endpoint = f"{volumes_endpoint}/{volume_id}/permissions"
    response = http.request(
        'PATCH',
        permissions_endpoint,
        body=json.dumps(permissions_config).encode('utf-8'),
        headers=headers
    )

    if response.status == 200:
        print("Permissions updated successfully.")
        return True
    else:
        print(f"Failed to update permissions. Status code: {response.status}")
        print(f"Error message: {response.data.decode('utf-8')}")
        return False

# Main execution
def main():
    volume_id = create_or_update_volume()
    if volume_id:
        update_permissions(volume_id)

if __name__ == "__main__":
    main()
