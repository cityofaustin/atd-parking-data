import os
import ntpath
import argparse
import logging
from datetime import datetime

from pypgrest import Postgrest
import pandas as pd
import boto3

import utils

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET_NAME = os.getenv("BUCKET_NAME")

POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")

S3_ENV = "prod"


def get_csv_list_for_processing(year, month, lastmonth, s3_client):
    """
    Parameters
    ----------
    year : Int
        Argument provided value for year.
    month : Int
        Argument provided value for month.
    lastmonth : Bool
        Argument that determines if the previous month should also be queried.
    client : boto3 client object
        For sending on to get_csv_list
    Returns
    -------
    csv_file_list : List
        A list of the csv files to be downloaded and upsert to Postgres.
    """
    # If args are missing, default to current month and/or year
    if not year:
        f_year = datetime.now().year
    else:
        f_year = year

    if not month:
        f_month = datetime.now().month
    else:
        f_month = month

    file_list = get_file_list(f_year, f_month, s3_client)

    if not month and not year:
        if lastmonth == True:
            prev_month = f_month - 1
            prev_year = f_year
            if prev_month == 0:
                prev_year = prev_year - 1
                prev_month = 12
            logger.debug(
                f"Getting data from folders: {prev_month}-{prev_year} and {f_month}-{f_year}"
            )
            prev_list = get_file_list(prev_year, prev_month, s3_client)
            file_list.extend(prev_list)
        else:
            logger.debug(f"Getting data from folders: {f_month}-{f_year}")

    file_list = [f for f in file_list if f.endswith(".json")]

    return file_list


def get_file_name(file_key):
    """
    Returns the name of an email file based on the full s3 file path
    :param file_key: the file path
    :return: string
    """
    return ntpath.basename(file_key)


def get_file_list(year, month, s3_client):
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings
    """
    csv_file_list = []
    pending_csv_list = aws_list_files(year, month, s3_client)
    for csv_file in pending_csv_list:
        csv_file_list.append(csv_file)

    # Finally return the final list
    return csv_file_list


def aws_list_files(year, month, client):
    """
    Returns a list of email files.
    :return: object
    """
    response = client.list_objects(
        Bucket=BUCKET_NAME, Prefix=f"app/{S3_ENV}/{str(year)}/{str(month)}",
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


def transform(passport):
    # Add "passport" to clarify where how the transaction was completed
    passport["source"] = "Passport - " + passport["Method"]
    passport["payment_method"] = passport["Payment Type"]
    ## Changes to default payment method names
    passport["payment_method"] = passport["payment_method"].str.replace(
        "Credit/Debit Card", "App - Credit Card", regex=True
    )

    passport["payment_method"] = passport["payment_method"].str.replace(
        "Zone Cash", "App - Wallet", regex=True
    )

    passport["payment_method"] = passport["payment_method"].str.replace(
        "Validation", "App - Validation", regex=True
    )

    passport["payment_method"] = passport["payment_method"].str.replace(
        "Free", "App - Free", regex=True
    )

    passport["payment_method"] = passport["payment_method"].str.replace(
        "Network Token", "App - Network Token", regex=True
    )

    # Convert string currency amounts to floats
    passport["amount"] = (
        passport["Parking Revenue"].str.replace("$", "", regex=True).astype(float)
    )
    passport["net_revenue"] = (
        passport["Net Revenue"].str.replace("$", "", regex=True).astype(float)
    )

    passport["start_time"] = pd.to_datetime(
        passport["Entry Time"],
        format="%Y/%m/%d %I:%M:%S %p",
        infer_datetime_format=True,
    )

    passport["end_time"] = pd.to_datetime(
        passport["Exit Time"], format="%Y/%m/%d %I:%M:%S %p", infer_datetime_format=True
    )

    passport["duration_min"] = (
        (passport["end_time"] - passport["start_time"]).dt.seconds
    ) / 60

    # Convert back to string for datetime fields
    passport["start_time"] = passport["start_time"].astype(str)
    passport["end_time"] = passport["end_time"].astype(str)

    # Renaming columns to match schema
    passport = passport.rename(
        columns={
            "Transaction #": "id",
            "Zone #": "zone_id",
            "Zone Group": "zone_group",
        }
    )

    # Ignore zones which were created for testing, they all contain AUS in the ID
    passport = passport[~passport["zone_id"].astype("str").str.contains("AUS")]

    # Dropping duplicate transaction IDs, which can happen for some unexplained reason
    # During testing, these looks like genuine dupes with same date/time/location
    passport = passport.drop_duplicates(subset=["id"], keep="last")

    # Subset of columns for aligning schema
    passport = passport[
        [
            "id",
            "zone_id",
            "zone_group",
            "payment_method",
            "start_time",
            "end_time",
            "duration_min",
            "amount",
            "net_revenue",
            "source",
        ]
    ]

    return passport


def to_postgres(df, client):
    # Upsert to database

    payload = df.to_dict(orient="records")
    try:
        res = client.upsert(resource="passport_transactions_raw", data=payload)
    except Exception as e:
        logger.error(client.res.text)
        raise e

    df = df[
        [
            "id",
            "payment_method",
            "zone_group",
            "zone_id",
            "duration_min",
            "start_time",
            "end_time",
            "amount",
            "source",
        ]
    ]

    payload = df.to_dict(orient="records")
    try:
        res = client.upsert(resource="transactions", data=payload)
    except Exception as e:
        logger.error(client.res.text)
        raise e

    return res


def main(args):
    s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    # Get list of JSON files and handle year/month args
    file_list = get_csv_list_for_processing(
        args.year, args.month, args.lastmonth, s3_client
    )

    # Go through all files and combine into a dataframe
    data = []
    for file in file_list:
        # Parse the file
        if ".json" in file:
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=file)
            # Read the JSON in each object
            df = pd.read_json(response.get("Body"))
            print("Loaded File: '%s'" % file)
            if not df.empty:
                df = transform(df)

                res = to_postgres(df, client)


# CLI arguments definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--year", type=int, help=f"Year of folder to select, defaults to current year",
)

parser.add_argument(
    "--month", type=int, help=f"Month of folder to select. defaults to current month",
)

parser.add_argument(
    "--lastmonth",
    type=bool,
    help=f"Will download from current month folder as well as previous.",
    default=False,
)

args = parser.parse_args()

logger = utils.get_logger(__file__, level=logging.DEBUG)

main(args)
