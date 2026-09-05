import os

import requests
from dotenv import load_dotenv

load_dotenv()


def get_access_token() -> dict:
    login_url = os.environ["SALESFORCE_LOGIN_URL"]
    client_id = os.environ["SALESFORCE_CLIENT_ID"]
    client_secret = os.environ["SALESFORCE_CLIENT_SECRET"]

    response = requests.post(
        f"{login_url}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )

    if not response.ok:
        print("Status:", response.status_code)
        print("Salesforce error:", response.text)
        response.raise_for_status()

    return response.json()


if __name__ == "__main__":
    token_data = get_access_token()
    print("Authentication successful")
    print(f"Instance URL: {token_data['instance_url']}")
    print(f"Token type: {token_data.get('token_type')}")