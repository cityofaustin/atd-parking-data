"""Fetch Flowbird meter transactions and load to S3"""
import argparse
import csv
from datetime import datetime, timezone, timedelta
import logging
import os
import time
from io import StringIO

import boto3
import requests

import utils

# env vars
USER = os.getenv("USER")
USER_PARD = os.getenv("USER_PARD")
PASSWORD = os.getenv("PASSWORD")
PASSWORD_PARD = os.getenv("PASSWORD_PARD")
ENDPOINT = os.getenv("ENDPOINT")
BUCKET = os.getenv("BUCKET")

# settings
ROOT_DIR = "meters"
DATE_FORMAT_API = "%Y%m%d000000"
DATE_FORMAT_HUMANS = "%Y-%m-%d"

# hack to validate response
HEADER_ROW_LENGTH = 396


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


def csv_string_as_dicts(csv_string):
    """Parse a CSV string into a list of dicts

    Args:
        csv_string (str): CSV string data 

    Returns:
        list: A list of dicts, one per CSV row
    """
    with StringIO(csv_string) as fin:
        reader = csv.DictReader(fin)
        return [row for row in reader]


def data_to_string(data):
    """Write a list of dicts as a CSV string

    Args:
        data (list): A list of dicts, one per CSV row

    Returns:
        str: The stringified csv data, with a header row
    """
    with StringIO() as fout:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        return fout.getvalue()


def remove_forbidden_keys(data, report):
    """Remove forbidden keys from data

    Args:
        data (list): A list of dictionaries, one per transactions

    Returns:
        list: A list of dictionaries, one per transaction, with forbidden keys removed
    """

    # There are different forbidden keys based on the report requested
    if report == "transaction_history":
        forbidden_keys = ["PLATE_NUMBER", "CARD_SERIAL_NUMBER"]
    else:
        forbidden_keys = []

    new_data = []
    for row in data:
        new_row = {k: v for k, v in row.items() if k.upper() not in forbidden_keys}
        new_data.append(new_row)
    return new_data


def format_file_key(chunk_start, env, report):
    """Format an S3 file path

    Args:
        chunk_start (str): A date string (in flowbird API query format)

    Returns:
        str: an S3 path + filename, aka the object key, in the format
          meters/transaction_history/year/month/<query-string>.csv
    """
    file_date = datetime.strptime(chunk_start, DATE_FORMAT_API)
    return f"{ROOT_DIR}/{env}/{report}/{file_date.year}/{file_date.month}/{chunk_start}.csv"


def main(args):
    start_date, end_date = handle_date_args(args.start, args.end)
    todos = get_todos(start_date, end_date)

    # Argument decides which table to get from the API, transactions or credit card payments
    if args.report == "transactions":
        report = "transaction_history"
    else:
        report = "archipel_transactionspub"

    # Argument to decide which account to use
    ## pard for Parks data which includes pool passes
    ## atd for parking meters
    if args.user == "pard":
        login_user = USER_PARD
        login_pass = PASSWORD_PARD
    else:
        login_user = USER
        login_pass = PASSWORD

    s3 = boto3.client("s3")

    for chunk_start in todos:
        chunk_end = format_chunk_end(chunk_start)
        # define query params
        # Different query params for the two types of tables
        if report == "transaction_history":
            data = {
                "startdate": chunk_start,
                "enddate": chunk_end,
                "report": report,
                "login": login_user,
                "password": login_pass,
            }
        else:
            data = {
                "startdatetime": chunk_start,
                "enddatetime": chunk_end,
                "report": report,
                "login": login_user,
                "password": login_pass,
            }

        # get data
        logger.debug(f"Fetching data from {chunk_start} to {chunk_end}")
        res = requests.post(ENDPOINT, data=data)
        res.raise_for_status()

        try:
            # the endpoint always returns status 200, even when access is denied, rate
            # limit error, etc :/
            # so let's make sure we have at least as much text as the header row
            assert len(res.text) >= HEADER_ROW_LENGTH
        except AssertionError:
            raise ValueError(
                f"Invalid data returned from flowbird endpoint: {res.text}"
            )

        data = csv_string_as_dicts(res.text)

        if data:

            data = remove_forbidden_keys(data, report)
            body = data_to_string(data)

            # upload to s3
            key = format_file_key(chunk_start, args.env, report)
            logger.debug(f"Uploading to s3: {key}")
            s3.put_object(Body=body, Bucket=BUCKET, Key=key)
        else:
            logger.debug(f"No data found for {chunk_start} to {chunk_end}")

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
        help=f"Date (in UTC) of the most recent records to be fetched (YYYY-MM-DD). Defaults to today",
    )

    parser.add_argument(
        "--report",
        default="transactions",
        choices=["transactions", "payments"],
        help=f"The type of report to collect (transactions, payments)",
    )

    parser.add_argument(
        "--user",
        default="atd",
        choices=["pard", "atd"],
        help=f"The user account to use to access data [atd (parking meters), pard (pool passes)]",
    )

    parser.add_argument(
        "-e", "--env", default="dev", choices=["dev", "prod"], help=f"The environment",
    )

    parser.add_argument(
        "-v", "--verbose", action="store_true", help=f"Sets logger to DEBUG level",
    )

    args = parser.parse_args()

    logger = utils.get_logger(
        __file__, level=logging.DEBUG if args.verbose else logging.INFO,
    )

    main(args)
