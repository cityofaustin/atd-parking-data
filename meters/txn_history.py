"""Fetch Flowbird meter transactions and load to S3.

One file is generated per day in the provided range and uploaded to S3 at:
<bucket>/meters/transaction_history/year/month/<query-start-date>.csv

Args:
    --start: Date (in UTC) of earliest records to be fetched (YYYY-MM-DD). Defaults to yesterday
    --end: Date (in UTC) of the most recent records to be fetched (YYYY-MM-DD). Defaults to yesterday
    -v/--verbose: Sets the logger level to DEBUG

Usage:
    $ python txn_history.py --start 2021-11-30 --verbose         
 """
import argparse
from datetime import datetime, timezone, timedelta
import logging
import os
import time

import boto3
import requests

import utils

# env vars
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
ENDPOINT = os.getenv("ENDPOINT")
BUCKET = os.getenv("BUCKET")

# settings
ROOT_DIR = "meters"
REPORT = "transaction_history"
DATE_FORMAT_API = "%Y%m%d000000"
DATE_FORMAT_HUMANS = "%Y-%m-%d"


def handle_date_args(start_string, end_string):
    """Parse or set default start and end dates from CLI args.

    Args:
        start_string (string): Date (in UTC) of earliest records to be fetched (YYYY-MM-DD).
            Defaults to yesterday.
        end_string (string): Date (in UTC) of most recent records to be fetched (YYYY-MM-DD).
            Defaults to yesterday.

    Returns:
        list: The start date and end date as python datetime objects
    """
    if start_string:
        # parse CLI arg date
        start_date = datetime.strptime(start_string, DATE_FORMAT_HUMANS).replace(tzinfo=timezone.utc)
    else:
        # create yesterday's date
        start_date = datetime.now(timezone.utc) - timedelta(days=1)

    if end_string:
        end_date = datetime.strptime(end_string, DATE_FORMAT_HUMANS).replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc) - timedelta(days=1)

    return start_date, end_date


def get_todos(start_date, end_date):
    """Generate a list of dates to be fetched.

    Args:
        start (datetime.datetime): The earliest day to process
        end (datetime.datetime): The last day to process
    Returns:
      list: a list of flowbird-API-friendly date strings that fall within (and including)
        the given given start/end
    """
    # adjust end date to midnight the next day
    end_date = end_date + timedelta(days=1)
    # calculate the # of days between start and end
    delta = end_date - start_date
    # generate a list of datetime objs within the the delta
    all_dates = [start_date + timedelta(days=x) for x in range(delta.days)]
    # format datestrings for API query
    return [dt.strftime(DATE_FORMAT_API) for dt in all_dates]


def format_chunk_end(chunk_start):
    """Add one day to the chunk start date so that it can be used as the query end date

    Args:
        chunk_start (str): A start date formatted in the flowbird API query format

    Returns:
        str: a date string in the flowbird API query format at 00:00:00 hours the next day
    """
    start_date = datetime.strptime(chunk_start, DATE_FORMAT_API) + timedelta(days=1)
    return datetime.strftime(start_date, DATE_FORMAT_API)


def format_file_key(chunk_start):
    """Format an S3 file path

    Args:
        chunk_start (str): A date string (in flowbird API query format)

    Returns:
        str: an S3 path + filename, aka the object key, in the format
          meters/transaction_history/year/month/<query-string>.csv
    """
    file_date = datetime.strptime(chunk_start, DATE_FORMAT_API)
    return f"{ROOT_DIR}/{REPORT}/{file_date.year}/{file_date.month}/{chunk_start}.csv"


def main(args):
    start_date, end_date = handle_date_args(args.start, args.end)
    todos = get_todos(start_date, end_date)

    s3 = boto3.client("s3")
    breakpoint()
    for chunk_start in todos:
        chunk_end = format_chunk_end(chunk_start)

        data = {
            "startdate": chunk_start,
            "enddate": chunk_end,
            "report": REPORT,
            "login": USER,
            "password": PASSWORD,
        }

        logger.debug(f"Fetching data from {chunk_start} to {chunk_end}")
        res = requests.post(ENDPOINT, data=data)
        res.raise_for_status()
        key = format_file_key(chunk_start)
        logger.debug(f"Uploading to s3: {key}")
        s3.put_object(Body=res.text, Bucket=BUCKET, Key=key)
        logger.debug(f"Sleeping to comply with rate limit...")
        time.sleep(61)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        type=str,
        help=f"Date (in UTC) of earliest records to be fetched (YYYY-MM-DD). Defaults to yesterday",
    )

    parser.add_argument(
        "--end",
        type=str,
        help=f"Date (in UTC) of the most recent records to be fetched (YYYY-MM-DD). Defaults to yesterday",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help=f"Sets logger to DEBUG level",
    )

    args = parser.parse_args()

    logger = utils.get_logger(
        __file__,
        level=logging.DEBUG if args.verbose else logging.INFO,
    )

    main(args)
