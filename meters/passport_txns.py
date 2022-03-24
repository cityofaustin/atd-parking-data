"""Fetch Flowbird meter transactions and load to S3"""
import argparse
from datetime import datetime, timezone, timedelta
import json
import logging
import os

import boto3
import requests

import utils


# Settings
ROOT_DIR = "app"
DATE_FORMAT_API = "%m/%d/%Y"
DATE_FORMAT_INPUT = "%Y-%m-%d"

# OpsMan Endpoints
LOGIN_URL = "https://ppprk.com/server/opmgmt/api/index.php/login"
REPORT_URL = "https://ppprk.com/server/opmgmt/api/reports_index.php/runcustomreport"

# OpsMan Credentials
USER = os.getenv("OPS_MAN_USER")
PASSWORD = os.getenv("OPS_MAN_PASS")

# AWS
AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET = os.getenv("BUCKET_NAME")


def validate_session(session):

    try:
        assert session.cookies.get("omsessiondata")
        assert session.cookies.get("PHPSESSID")
    except Exception as e:
        raise ValueError(f"Session failed validation: {str(e)}")
    return True


def start_session(user, password, url):
    payload = {
        "username": user,
        "password": password,
        "setsessions": "1",
    }
    params = {
        "timezonename": "Etc%2FGMT-6",
    }
    session = requests.Session()
    res = session.post(url, json=payload, params=params)
    res.raise_for_status()
    validate_session(session)
    return session


def get_report_params(session):
    return {
        "report_id": 295,
        "timezonename": "Etc/GMT-6",
    }


def get_report_payload(start_date, start_count, page_size):
    return {
        "startdate": start_date.strftime(DATE_FORMAT_API),
        "enddate": start_date.strftime(DATE_FORMAT_API),
        "locale": "en",
        "operator_id": [550],
        "zone_id": [],
        "start": start_count,
        "count": page_size,
    }


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
        start_date = datetime.strptime(start_string, DATE_FORMAT_INPUT).replace(
            tzinfo=timezone.utc
        )
    else:
        # create yesterday's date
        start_date = datetime.now(timezone.utc) - timedelta(days=1)

    if end_string:
        # parse CLI arg date
        end_date = datetime.strptime(end_string, DATE_FORMAT_INPUT).replace(
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
    return [start_date + timedelta(days=x) for x in range(delta.days)]


def remove_forbidden_keys(data):
    """Remove forbidden keys from datta

    Args:
        data (list): A list of dictionaries, one per transactions

    Returns:
        list: A list of dictionariess, one per transaction, with forbidden keys removed
    """

    # There are different forbidden keys based on the report requested
    forbidden_keys = ["customer id", "space/lpn"]
    new_data = []
    for row in data:
        new_row = {k: v for k, v in row.items() if k.lower() not in forbidden_keys}
        new_data.append(new_row)
    return new_data


def format_file_key(file_date, env):
    """Format an S3 file path

    Args:
        chunk_start (str): A date string (in flowbird API query format)

    Returns:
        str: an S3 path + filename, aka the object key, in the format
          meters/transaction_history/year/month/<query-string>.json
    """
    return f"{ROOT_DIR}/{env}/{file_date.year}/{file_date.month}/{file_date.strftime(DATE_FORMAT_INPUT)}.json"


def main(args):
    # Format arguments and get list of dates
    start_date, end_date = handle_date_args(args.start, args.end)
    todos = get_todos(start_date, end_date)

    # Log in to OpsMan
    session = start_session(USER, PASSWORD, LOGIN_URL)

    # AWS log in
    s3 = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    # Get request params
    params = get_report_params(session)

    for chunk_start in todos:
        stop = False
        start_count = 0
        page_size = 200
        records = []

        # Results paginated, so go through each page and download the data
        while not stop:
            # get data
            logger.debug(
                f"Fetching records for {chunk_start} starting at {start_count}"
            )
            payload = get_report_payload(chunk_start, start_count, page_size)
            res = session.post(REPORT_URL, json=payload, params=params)
            res.raise_for_status()

            # Handle data
            data = res.json()
            current_records = data["data"]
            records.extend(current_records)
            total_record_count = data["count"]
            current_record_count = len(current_records)
            logger.debug(f"Found: {current_record_count} records")
            logger.debug(f"{len(records)} out of {total_record_count} downloaded")

            # stop condition and go to next page
            start_count += page_size
            stop = len(records) >= total_record_count

        # Drop fields we don't need
        records = remove_forbidden_keys(records)
        key = format_file_key(chunk_start, args.env)

        # Send to S3 bucket
        logger.debug(f"Uploading to s3: {key}")
        s3.put_object(Body=json.dumps(records), Bucket=BUCKET, Key=key)


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
