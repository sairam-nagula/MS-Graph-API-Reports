import time
import requests
import json
import msal
import os
from dotenv import load_dotenv

# Load secrets
load_dotenv()
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]
GRAPH_API = "https://graph.microsoft.com/v1.0"

# Replace with group IDs
GROUP_IDS = [
    "193b241e-13ce-4cef-bc66-cad7a3c1640f",  # L-E3+Teams Users
    
]

def get_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" not in result:
        raise Exception("Could not get token")
    return result["access_token"]

def create_user(token, user_data):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(f"{GRAPH_API}/users", headers=headers, json=user_data)
    response.raise_for_status()
    return response.json()["id"]

def assign_manager(token, user_id, manager_email):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Look up manager's Object ID from email
    lookup_url = f"{GRAPH_API}/users/{manager_email}"
    lookup_resp = requests.get(lookup_url, headers=headers)
    lookup_resp.raise_for_status()
    manager_id = lookup_resp.json()["id"]

    # Assign manager by Object ID
    assign_url = f"{GRAPH_API}/users/{user_id}/manager/$ref"
    body = {
        "@odata.id": f"https://graph.microsoft.com/v1.0/users/{manager_id}"
    }
    assign_resp = requests.put(assign_url, headers=headers, json=body)
    assign_resp.raise_for_status()

def add_user_to_groups(token, user_id):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    for group_id in GROUP_IDS:
        url = f"{GRAPH_API}/groups/{group_id}/members/$ref"
        body = {
            "@odata.id": f"https://graph.microsoft.com/v1.0/users/{user_id}"
        }
        response = requests.post(url, headers=headers, json=body)
        response.raise_for_status()





def main():
    print("\n New User Onboarding Tool \n")

    display_name = input("Enter full name (e.g., John Doe): ").strip()
    username = input("Enter username (e.g., johndoe): ").strip()
    domain = input("Enter email domain (e.g., margaritavilleatsea.com): ").strip()
    password = input("Enter temporary password (e.g., CruiseToday!): ").strip()
    manager_email = input("Enter manager's email address: ").strip()
    job_title = input("Enter job title: ").strip()
    department = input("Enter department: ").strip()
    office_location = input("Enter office location: ").strip()

    token = get_token()

    user_data = {
        "accountEnabled": True,
        "displayName": display_name,
        "mailNickname": username,
        "userPrincipalName": f"{username}@{domain}",
        "passwordProfile": {
            "forceChangePasswordNextSignIn": True,
            "password": password
        },
        "jobTitle": job_title,
        "department": department,
        "officeLocation": office_location
    }

    print("\nCreating user...")
    user_id = create_user(token, user_data)
    print(" User ID:", user_id)
    print(" User created.")
    
    print("\nAssigning licenses...")
    time.sleep(10)  # Simulate license assignment delay

    print("Assigning manager...")
    assign_manager(token, user_id, manager_email)
    print(" Manager assigned.")

    print("Adding user to groups...")

    add_user_to_groups(token, user_id)
    print(" User added to groups.")

    print("\n Done! The user has been fully onboarded.\n")

if __name__ == "__main__":
    main()
