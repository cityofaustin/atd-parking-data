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
load_dotenv("socrata.env")
DATE_FORMAT_HUMANS = "%Y-%m-%d"

# Credentials
POSTGREST_TOKEN = os.environ.get("POSTGREST_TOKEN")
SO_WEB = os.environ.get("SO_WEB")
SO_TOKEN = os.environ.get("SO_TOKEN")
SO_USER = os.environ.get("SO_USER")
SO_PASS = os.environ.get("SO_PASS")

# Socrata dataset IDs
FISERV_DATASET = os.environ.get("FISERV_DATASET")
METERS_DATASET = os.environ.get("METERS_DATASET")
PAYMENTS_DATASET = os.environ.get("PAYMENTS_DATASET")
TXNS_DATASET = os.environ.get("TXNS_DATASET")


def tzcleanup(data):

    # Truncate the updated_at field timestamp to remove the timezone info for Socrata
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


def fiserv(start, end, pstgrs, soda):
    ## Fiserv reports upsert to socrata
    logger.debug(f"Publishing Fiserv data to Socrata from {start} to {end}")

    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "invoice_id",
    }

    response = pstgrs.select(resource="fiserv_reports_raw", params=params)

    response = tzcleanup(response)

    soda.upsert(FISERV_DATASET, response)


def meters(start, end, pstgrs, soda):
    ## Flowbird transactions upsert to socrata
    logger.debug(f"Publishing parking meter data to Socrata from {start} to {end}")

    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "id",
    }

    response = pstgrs.select(resource="flowbird_transactions_raw", params=params)

    response = tzcleanup(response)

    soda.upsert(METERS_DATASET, response)


def payments(start, end, pstgrs, soda):
    ## Flowbird payment upsert to socrata
    logger.debug(
        f"Publishing parking meter payment data to Socrata from {start} to {end}"
    )
    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "invoice_id",
    }

    response = pstgrs.select(resource="flowbird_payments_raw", params=params)

    response = tzcleanup(response)

    soda.upsert(PAYMENTS_DATASET, response)


def transactions(start, end, pstgrs, soda):
    ## Combined flowbird + passport transactions data
    logger.debug(
        f"Publishing parking transaction data to Socrata from {start} to {end}"
    )
    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "id",
    }

    response = pstgrs.select(resource="transactions", params=params)

    response = tzcleanup(response)

    soda.upsert(TXNS_DATASET, response)


def upsert_all(start, end, pstgrs, soda):
    # Publishes all datasets to socrata
    fiserv(start, end, pstgrs, soda)
    meters(start, end, pstgrs, soda)
    payments(start, end, pstgrs, soda)
    transactions(start, end, pstgrs, soda)


def main(args):
    pstgrs = Postgrest(
        "http://127.0.0.1:3000",
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=500,)

    start_date, end_date = handle_date_args(args.start, args.end)

    if args.dataset:
        if args.dataset == "fiserv":
            fiserv(start_date, end_date, pstgrs, soda)

        if args.dataset == "meters":
            meters(start_date, end_date, pstgrs, soda)

        if args.dataset == "payments":
            payments(start_date, end_date, pstgrs, soda)

        if args.dataset == "transactions":
            transactions(start_date, end_date, pstgrs, soda)

        if args.dataset == "all":
            upsert_all(start_date, end_date, pstgrs, soda)

    else:
        upsert_all(start_date, end_date, pstgrs, soda)


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

logger = utils.get_logger(
    __file__, level=logging.DEBUG if args.verbose else logging.INFO,
)

main(args)
