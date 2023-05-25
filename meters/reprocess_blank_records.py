# Standard Library imports
import os
import argparse
from datetime import datetime, timezone, timedelta
import logging
from config.location_names import METER_LOCATION_NAMES

import pandas as pd
from pypgrest import Postgrest

# Envrioment variables
DATE_FORMAT_HUMANS = "%Y-%m-%d"

# Credentials
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_USER = os.getenv("SO_USER")
SO_PASS = os.getenv("SO_PASS")
table = "flowbird_transactions_raw"

def create_location_name(row):
    id = row["meter_id"]
    for id_range in METER_LOCATION_NAMES:
        if id in id_range:
            return METER_LOCATION_NAMES[id_range]
    return "Unknown Location"


pstgrs = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

paginate = True
offset = 0
while paginate:
    params = {
        "select": "*",
        "and": f"(location_name.is.null)",
        "order": "id",
        "limit": 1000,
        "offset": offset,
    }
    offset += 0
    response = pstgrs.select(resource=table, params=params, pagination=True)
    smartfolio = pd.DataFrame(response)
    if len(response) == 0:
        paginate = False
    else:
        if offset % 10000 == 0:
            print(f"Uploading chunks: {offset} records so far")
        # Get location names based on the meter ID
        smartfolio["location_name"] = smartfolio.apply(create_location_name, axis=1)
        payload = smartfolio.to_dict(orient="records")

        res = pstgrs.upsert(resource=table, data=payload)

        smartfolio = smartfolio[
            [
                "id",
                "payment_method",
                "meter_id",
                "duration_min",
                "start_time",
                "end_time",
                "amount",
                "location_name",
            ]
        ]
        smartfolio["source"] = "Parking Meters"
        payload = smartfolio.to_dict(orient="records")
        res = pstgrs.upsert(resource="transactions", data=payload)