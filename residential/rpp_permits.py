import logging
import requests
import sodapy
import os

from utils import chunks

PASSPORT_CLIENT_ID = os.getenv("PASSPORT_CLIENT_ID")
PASSPORT_CLIENT_SECRET = os.getenv("PASSPORT_CLIENT_SECRET")
PASSPORT_OPERATOR_ID = os.getenv("PASSPORT_OPERATOR_ID")
PASSPORT_AUTH_ENDPOINT = os.getenv("PASSPORT_AUTH_ENDPOINT")
PASSPORT_ENFORCEMENT_ENDPOINT = os.getenv("PASSPORT_ENFORCEMENT_ENDPOINT")
PASSPORT_ZONES_ENDPOINT = os.getenv("PASSPORT_ZONES_ENDPOINT")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")


def get_bearer_token():
    request_definition = {
        "grant_type": "client_credentials",
        "client_id": PASSPORT_CLIENT_ID,
        "client_secret": PASSPORT_CLIENT_SECRET,
        "audience": "public.api.passportinc.com"
    }
    token_response = requests.post(PASSPORT_AUTH_ENDPOINT, json=request_definition)
    if token_response.status_code == 200:
        return token_response.json()["access_token"]
    else:
        return token_response.text


def get_license_plate_records(bearer_token):
    parking_rights_url = PASSPORT_ENFORCEMENT_ENDPOINT
    passport_headers = {"Authorization": f"Bearer {bearer_token}"}
    passport_params = {"operator_id": PASSPORT_OPERATOR_ID}
    passport_response = requests.get(parking_rights_url, headers=passport_headers, params=passport_params)

    return passport_response.json()["data"]


def format_plate_records(passport_records):
    socrata_upload_data = []
    for record in passport_records:
        socrata_upload_data.append({
            "id": record["id"],
            "vehicle_plate": record["vehicle"]["vehicle_plate"],
            "vehicle_state": record["vehicle"]["vehicle_state"],
        })
    return socrata_upload_data


def main():
    bearer_token = get_bearer_token()
    passport_records = get_license_plate_records(bearer_token)

    socrata_client = sodapy.Socrata(
        "datahub.austintexas.gov",
        SO_TOKEN,
        username=SO_USER,
        password=SO_PASS,
        timeout=500,
    )

    resource_id = "p9tg-r2i3"
    # metadata_socrata = socrata_client.get_metadata(resource_id)

    payload = format_plate_records(passport_records)

    method = "replace"

    for chunk in chunks(payload, 1000):
        if method == "replace":
            # replace the dataset with first chunk
            # subsequent chunks will be upserted
            socrata_client.replace(resource_id, chunk)
            method = "upsert"
        else:
            socrata_client.upsert(resource_id, chunk)

    return len(payload)
    # logger.info(f"{len(payload)} records processed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    script_result = main()
    print(script_result)

