import requests

from src.salesforce_cdc.auth import get_access_token


def query_opportunities():
    token_data = get_access_token()

    instance_url = token_data["instance_url"]
    access_token = token_data["access_token"]

    soql = """
        SELECT Id, Name, StageName, Amount, CloseDate, LastModifiedDate
        FROM Opportunity
        ORDER BY LastModifiedDate DESC
        LIMIT 20
    """

    response = requests.get(
        f"{instance_url}/services/data/v65.0/query",
        headers={
            "Authorization": f"Bearer {access_token}",
        },
        params={
            "q": soql,
        },
        timeout=30,
    )

    if not response.ok:
        print("Status:", response.status_code)
        print("Salesforce error:", response.text)
        response.raise_for_status()

    return response.json()


if __name__ == "__main__":
    result = query_opportunities()

    print(f"Total records: {result['totalSize']}")

    for record in result["records"]:
        print(
            record["Id"],
            record["Name"],
            record["StageName"],
            record.get("Amount"),
        )