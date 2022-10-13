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


def chunks(lst, n):
    """
    Helper function for breaking large lists into chunks for uploading batches
    Parameters
    ----------
    lst - a fairly long list that will be broken into chunks
    n - length of the chunks that will be returned

    Returns
    -------
    Generator that yields smaller lists of length n
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


def batch_upload(soda, dataset, response):
    """
    Uploads data to Socrata in batches if the length of our data is too large in order to avoid timeouts
    Parameters
    ----------
    soda - SodaPy client object
    dataset - The ID of the Socrata dataset to upload to
    response - A list of dictionaries of data returned from the Postgres DB

    Returns
    None
    -------

    """
    logger.debug(f"Data too large, uploading data in chunks to {dataset}")

    for chunk in chunks(response, 1000):
        logger.debug(f"Uploading chunk...")
        soda.upsert(dataset, chunk)


def fiserv(start, end, pstgrs, soda):
    """Queries the postgres database for Fiserv data and sends the response to Socrata.
    
    Args:
        start (string): Inclusive date (UTC) of earliest records to be uploaded
        end (string): Inclusive date (UTC) of latest records to be uploaded
        postgrest and sodapy client objects
    
    Returns:
        none
    """
    logger.debug(f"Publishing Fiserv data to Socrata from {start} to {end}")

    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "invoice_id",
    }

    response = pstgrs.select(resource="fiserv_reports_raw", params=params)

    response = tzcleanup(response)

    response = remove_forbidden_keys(response)

    if len(response) > 1000:
        batch_upload(soda, FISERV_DATASET, response)
    else:
        soda.upsert(FISERV_DATASET, response)


def meters(start, end, pstgrs, soda):
    """Queries the postgres database for parking meter (smartfolio) data,
        and sends the response to Socrata.
    
    Args:
        start (string): Inclusive date (UTC) of earliest records to be uploaded
        end (string): Inclusive date (UTC) of latest records to be uploaded
        postgrest and sodapy client objects
    
    Returns:
        none
    """
    logger.debug(f"Publishing parking meter data to Socrata from {start} to {end}")

    params = {
        "select": "*",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
        "order": "id",
    }

    response = pstgrs.select(resource="flowbird_transactions_raw", params=params)

    response = tzcleanup(response)

    if len(response) > 1000:
        batch_upload(soda, METERS_DATASET, response)
    else:
        soda.upsert(METERS_DATASET, response)


def payments(start, end, pstgrs, soda):
    """Queries the postgres database for parking meter credit card payment (smartfolio) data,
        and sends the response to Socrata.
    
    Args:
        start (string): Inclusive date (UTC) of earliest records to be uploaded
        end (string): Inclusive date (UTC) of latest records to be uploaded
        postgrest and sodapy client objects
    
    Returns:
        none
    """
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

    response = remove_forbidden_keys(response)

    if len(response) > 1000:
        batch_upload(soda, PAYMENTS_DATASET, response)
    else:
        soda.upsert(PAYMENTS_DATASET, response)


def transactions(start, end, pstgrs, soda):
    """Queries the postgres database for the combined parking transaction data (passport + smartfolio),
        and sends the response to Socrata.
    
    Args:
        start (string): Inclusive date (UTC) of earliest records to be uploaded
        end (string): Inclusive date (UTC) of latest records to be uploaded
        postgrest and sodapy client objects
    
    Returns:
        none
    """
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

    if len(response) > 1000:
        batch_upload(soda, TXNS_DATASET, response)
    else:
        soda.upsert(TXNS_DATASET, response)


def upsert_all(start, end, pstgrs, soda):
    """Runs all four dataset publishing functions
    
    Args:
        start (string): Inclusive date (UTC) of earliest records to be uploaded
        end (string): Inclusive date (UTC) of latest records to be uploaded
        postgrest and sodapy client objects
    
    Returns:
        none
    """

    fiserv(start, end, pstgrs, soda)
    meters(start, end, pstgrs, soda)
    payments(start, end, pstgrs, soda)
    transactions(start, end, pstgrs, soda)


def remove_forbidden_keys(data):
    """Remove forbidden keys from data that are not needed in Socrata

    Args:
        data (list): A list of dictionaries, one per transactions

    Returns:
        list: A list of dictionariess, one per transaction, with forbidden keys removed
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
    soda = Socrata(SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=500, )

    # format date arguments
    start_date, end_date = handle_date_args(args.start, args.end)

    # CLI argument logic
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

    # If no dataset argument then publish all
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

logger = utils.get_logger(__file__, level=logging.DEBUG)

main(args)
