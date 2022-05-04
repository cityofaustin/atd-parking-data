# Standard Library imports
import os
import argparse
from datetime import datetime, timezone, timedelta
import logging

# Related third-party imports
from sodapy import Socrata
from pypgrest import Postgrest
from dotenv import load_dotenv

import utils

# Envrioment variables
DATE_FORMAT_HUMANS = "%Y-%m-%d"

# Credentials
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")

# Socrata dataset IDs
FISERV_DATASET = os.getenv("FISERV_DATASET")
METERS_DATASET = os.getenv("METERS_DATASET")
PAYMENTS_DATASET = os.getenv("PAYMENTS_DATASET")
TXNS_DATASET = os.getenv("TXNS_DATASET")


def handle_date_args(start_string, end_string):
    """Parse or set default start and end dates from CLI args.

    Args:
        start_string (string): Date (in UTC) of earliest records to be fetched (YYYY-MM-DD).
            Defaults to yesterday.
        end_string (string): Date (in UTC) of most recent records to be fetched (YYYY-MM-DD).
            Defaults to today.
    
    Returns:
        list: The start date and end date as python datetime objects
    """
    if start_string:
        # parse CLI arg date
        start_date = datetime.strptime(start_string, DATE_FORMAT_HUMANS).replace(
            tzinfo=timezone.utc
        )
    else:
        # create yesterday's date
        start_date = datetime.now(timezone.utc) - timedelta(days=2)

    if end_string:
        # parse CLI arg date
        end_date = datetime.strptime(end_string, DATE_FORMAT_HUMANS).replace(
            tzinfo=timezone.utc
        )
    else:
        # create today's date
        end_date = datetime.now(timezone.utc)

    return start_date, end_date


def tzcleanup(data):
    """Removes timezone from a postgres datetime field for upload to socrata.
        Socrata data type is a floating timestamp which does not include timezone.
    
    Args:
        Data (list of dicts): Data response from postgres.
    
    Returns:
        Data (list of dicts): The response with the updated_at column without timezone info/
    """
    for row in data:
        row["updated_at"] = row["updated_at"][:19]
    return data


pstgrs = Postgrest(
    POSTGREST_ENDPOINT,
    token=POSTGREST_TOKEN,
    headers={"Prefer": "return=representation"},
)
# sodapy
soda = Socrata(SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=500,)
start_date = None
end_date = None
# format date arugments
start_date, end_date = handle_date_args(start_date, end_date)

start = start_date
end = end_date

offset = 0

while True:
    print(
        f"Publishing parking transaction data to Socrata from {offset} to {offset + 1000}"
    )
    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "id",
        "offset": offset,
        "limit": 10000,
    }

    response = pstgrs.select(resource="transactions", params=params)

    response = tzcleanup(response)

    soda.upsert(TXNS_DATASET, response)

    offset = offset + 10000
