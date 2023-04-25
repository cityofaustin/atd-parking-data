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

DATASETS = {
    "fiserv_reports_raw": FISERV_DATASET,
    "flowbird_transactions_raw": METERS_DATASET,
    "flowbird_payments_raw": PAYMENTS_DATASET,
    "transactions": TXNS_DATASET,
}


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
        start_date = datetime.now(timezone.utc) - timedelta(days=1)

    if end_string:
        # parse CLI arg date
        end_date = datetime.strptime(end_string, DATE_FORMAT_HUMANS).replace(
            tzinfo=timezone.utc
        )
    else:
        # create today's date
        end_date = datetime.now(timezone.utc)

    return start_date, end_date


def batch_upload(start, end, pstgrs, soda, table):
    """
    Uploads data to Socrata in batches of 1,000 records.
    Parameters
    ----------
    start (string): Inclusive date (UTC) of earliest records to be uploaded (updated_at)
    end (string): Inclusive date (UTC) of latest records to be uploaded (updated_at)
    pstgrs: Postgrest client object
    soda: SodaPy client object
    table (string): The name of the table in postgres we are uploading

    Returns
    None
    -------

    """
    logger.debug(f"Publishing table: {table} to Socrata from {start} to {end}")
    paginate = True
    offset = 0
    while paginate:
        params = {
            "select": "*",
            "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
            "order": "id",
            "limit": 10000,
            "offset": offset,
        }
        offset += 10000
        response = pstgrs.select(resource=table, params=params, pagination=True)
        if len(response) == 0:
            paginate = False
        else:
            response = tzcleanup(response)
            response = remove_forbidden_keys(response)
            logger.debug(f"Uploading chunks: {offset} records so far")
            soda.upsert(DATASETS[table], response)


def remove_forbidden_keys(data):
    """Remove forbidden keys from data that are not needed in Socrata

    Args:
        data (list): A list of dictionaries, one per transactions

    Returns:
        list: A list of dictionaries, one per transaction, with forbidden keys removed
    """

    # There are different forbidden keys based on the report requested
    forbidden_keys = ["MATCH_FIELD"]

    new_data = []
    for row in data:
        new_row = {k: v for k, v in row.items() if k.upper() not in forbidden_keys}
        new_data.append(new_row)
    return new_data


def main(args):
    ## Client objects
    # postgrest
    pstgrs = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )
    # sodapy
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=500,)

    # format date arguments
    start_date, end_date = handle_date_args(args.start, args.end)

    # CLI argument logic
    if args.dataset:
        if args.dataset == "fiserv":
            batch_upload(start_date, end_date, pstgrs, soda, "fiserv_reports_raw")

        if args.dataset == "meters":
            batch_upload(
                start_date, end_date, pstgrs, soda, "flowbird_transactions_raw"
            )

        if args.dataset == "payments":
            batch_upload(start_date, end_date, pstgrs, soda, "flowbird_payments_raw")

        if args.dataset == "transactions":
            batch_upload(start_date, end_date, pstgrs, soda, "transactions")

        if args.dataset == "all":
            batch_upload(start_date, end_date, pstgrs, soda, "fiserv_reports_raw")
            batch_upload(
                start_date, end_date, pstgrs, soda, "flowbird_transactions_raw"
            )
            batch_upload(start_date, end_date, pstgrs, soda, "flowbird_payments_raw")
            batch_upload(start_date, end_date, pstgrs, soda, "transactions")

    # If no dataset argument then publish all
    else:
        batch_upload(start_date, end_date, pstgrs, soda, "fiserv_reports_raw")
        batch_upload(start_date, end_date, pstgrs, soda, "flowbird_transactions_raw")
        batch_upload(start_date, end_date, pstgrs, soda, "flowbird_payments_raw")
        batch_upload(start_date, end_date, pstgrs, soda, "transactions")


# CLI arguments definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--dataset",
    type=str,
    default="all",
    choices=["fiserv", "meters", "payments", "transactions", "all"],
    help=f"Dataset Name to upload to Socrata (fiserv, meters, payments, transactions, all)",
)

parser.add_argument(
    "--start",
    type=str,
    help=f"Date (in UTC) of earliest records to be uploaded (YYYY-MM-DD). Defaults to yesterday",
)

parser.add_argument(
    "--end",
    type=str,
    help=f"Date (in UTC) of the most recent records to be uploaded (YYYY-MM-DD). Defaults to today",
)

args = parser.parse_args()

logger = utils.get_logger(__file__, level=logging.DEBUG)

main(args)
