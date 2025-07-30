import requests
import json

# --- Config ---
DATABRICKS_INSTANCE = "https://<your-databricks-instance>"  # e.g. https://dbc-1234.cloud.databricks.com
TOKEN = "<your-databricks-pat>"
GROUP_NAME = "data-engineers"              # Target group to create or validate
GROUP_MANAGER_USER_NAME = "manager@example.com"  # User email for manager
GROUP_MANAGER_GROUP_NAME = "engineering-admins"  # Another group with group manager privilege

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# --- Step 0: List all groups ---
def list_all_groups():
    groups = []
    start_index = 1
    count = 100
    while True:
        response = requests.get(
            f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Groups?startIndex={start_index}&count={count}",
            headers=HEADERS
        )
        if response.status_code != 200:
            raise Exception(f"Failed to fetch groups: {response.status_code} - {response.text}")

        data = response.json()
        resources = data.get("Resources", [])
        groups.extend(resources)

        if len(resources) < count:
            break
        start_index += count

    return groups

# --- Step 1: Check if group exists ---
def find_group(group_name):
    response = requests.get(
        f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Groups?filter=displayName eq \"{group_name}\"",
        headers=HEADERS
    )
    if response.status_code != 200:
        raise Exception(f"Failed to check group: {response.status_code} - {response.text}")
    resources = response.json().get("Resources", [])
    return resources[0] if resources else None

# --- Step 2: Create group ---
def create_group(group_name):
    payload = {"displayName": group_name}
    response = requests.post(
        f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Groups",
        headers=HEADERS,
        data=json.dumps(payload)
    )
    if response.status_code == 201:
        return response.json()
    else:
        raise Exception(f"Failed to create group: {response.status_code} - {response.text}")

# --- Step 3: Get user ID ---
def get_user_id(user_name):
    response = requests.get(
        f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Users?filter=userName eq \"{user_name}\"",
        headers=HEADERS
    )
    if response.status_code != 200:
        raise Exception(f"Failed to fetch user: {response.status_code} - {response.text}")

    resources = response.json().get("Resources", [])
    if not resources:
        raise Exception(f"User '{user_name}' not found in workspace")
    return resources[0]["id"]

# --- Step 4: Add member (user or group) to group ---
def add_member_to_group(group_id, member_id):
    payload = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {"op": "add", "path": "members", "value": [{"value": member_id}]}
        ]
    }
    response = requests.patch(
        f"{DATABRICKS_INSTANCE}/api/2.0/preview/scim/v2/Groups/{group_id}",
        headers=HEADERS,
        data=json.dumps(payload)
    )
    if response.status_code == 200:
        print(f"✅ Added member {member_id} to group {group_id}")
    elif response.status_code == 409:
        print(f"⚠️ Member {member_id} already in group {group_id}")
    else:
        raise Exception(f"Failed to add member: {response.status_code} - {response.text}")

# --- Step 5: Set permissions for manager group on the created group ---
def set_group_permissions(target_group_id, manager_group_name):
    permission_payload = {
        "access_control_list": [
            {
                "group_name": manager_group_name,
                "permission_level": "CAN_MANAGE"
            }
        ]
    }
    response = requests.patch(
        f"{DATABRICKS_INSTANCE}/api/2.0/permissions/groups/{target_group_id}",
        headers=HEADERS,
        data=json.dumps(permission_payload)
    )
    if response.status_code == 200:
        print(f"✅ Set CAN_MANAGE permission for '{manager_group_name}' on group {target_group_id}")
    else:
        raise Exception(f"Failed to set permissions: {response.status_code} - {response.text}")

# --- Step 6: Add group to workspace with CAN_USE permission ---
def add_group_to_workspace(group_name):
    permission_payload = {
        "access_control_list": [
            {
                "group_name": group_name,
                "permission_level": "CAN_USE"
            }
        ]
    }
    response = requests.patch(
        f"{DATABRICKS_INSTANCE}/api/2.0/permissions/workspace",
        headers=HEADERS,
        data=json.dumps(permission_payload)
    )
    if response.status_code == 200:
        print(f"✅ Added group '{group_name}' to workspace with CAN_USE permission")
    else:
        raise Exception(f"Failed to add group to workspace: {response.status_code} - {response.text}")

# --- Main Logic ---
try:
    # 1. List all groups
    all_groups = list_all_groups()
    print(f"📋 Found {len(all_groups)} groups in workspace.")

    # 2. Check or create the target group
    group = find_group(GROUP_NAME)
    if group:
        print(f"⚠️ Group '{GROUP_NAME}' already exists (ID: {group['id']})")
    else:
        group = create_group(GROUP_NAME)
        print(f"✅ Group '{GROUP_NAME}' created (ID: {group['id']})")

    group_id = group["id"]

    # 3. Add manager user to the group
    user_id = get_user_id(GROUP_MANAGER_USER_NAME)
    add_member_to_group(group_id, user_id)

    # 4. Add manager group to the group
    manager_group = find_group(GROUP_MANAGER_GROUP_NAME)
    if not manager_group:
        raise Exception(f"Manager group '{GROUP_MANAGER_GROUP_NAME}' not found.")
    add_member_to_group(group_id, manager_group["id"])

    # 5. Set CAN_MANAGE permission for the manager group
    set_group_permissions(group_id, GROUP_MANAGER_GROUP_NAME)

    # 6. Add created group to workspace
    add_group_to_workspace(GROUP_NAME)

except Exception as e:
    print(f"❌ Error: {e}")
