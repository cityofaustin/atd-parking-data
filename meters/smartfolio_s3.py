import ntpath
import os
import argparse
from datetime import datetime
import logging

from pypgrest import Postgrest
import pandas as pd
import boto3
from dotenv import load_dotenv

import utils

# Envrioment variables

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")


def handle_year_month_args(year, month, lastmonth, aws_s3_client):
    """

    Parameters
    ----------
    year : Int
        Argument provided value for year.
    month : Int
        Argument provided value for month.
    lastmonth : Bool
        Argument that determines if the previous month should also be queried.
    aws_s3_client : boto3 client object
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

    csv_file_list = get_csv_list(f_year, f_month, aws_s3_client)

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
            prev_list = get_csv_list(prev_year, prev_month, aws_s3_client)
            csv_file_list.extend(prev_list)
        else:
            logger.debug(f"Getting data from folders: {f_month}-{f_year}")

    csv_file_list = [f for f in csv_file_list if f.endswith(".csv")]

    return csv_file_list


def get_file_name(file_key):
    """
    Returns the name of an email file based on the full s3 file path
    :param file_key: the file path
    :return: string
    """
    return ntpath.basename(file_key)


def get_csv_list(year, month, client):
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings
    """
    csv_file_list = []
    pending_csv_list = aws_list_files(year, month, client)
    for csv_file in pending_csv_list:
        csv_file_list.append(csv_file)

    # Remove the first item, it is not needed
    # since it is just the name of the folder
    csv_file_list.pop(0)

    # Finally return the final list
    return csv_file_list


def aws_list_files(year, month, client):
    """
    Returns a list of email files.
    :return: object
    """
    response = client.list_objects(
        Bucket=BUCKET_NAME,
        Prefix="meters/prod/transaction_history/" + str(year) + "/" + str(month),
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


def get_invoice_id(banking_id, terminal_code):
    """Create the Inovice ID which is a concatention of the banking ID and device ID

    Args:
        banking_id (int): whatever this is
        terminal_code (int): whatever this is

    Returns:
        int: The formatted invoice ID
    """
    if pd.isna(banking_id) and terminal_code:
        return -1

    if banking_id == 0:
        return -1

    # get last 4 digits of terminal code
    terminal_code = str(terminal_code)[-4:]

    # zero-pad bank ID to 6 digits
    banking_id = f"{banking_id:06d}"
    invoice_id = f"{terminal_code}{banking_id}"
    return int(invoice_id)


def postgres_datetime(time_field):
    """Changes the existing datetime field in S3 to a format that can be stored by postgres.
        First parses the string time as datetime type then outputs as string.
    
    Args:
        time_field (string): Datetime field used by smartfolio. 
        Sent in a lambda function from a pandas series.
    
    Returns:
        output (string): Formatted datetime field that is compatable with postgres
    """

    output = pd.to_datetime(
        time_field, format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True
    )
    return str(output)


def transform(smartfolio):
    """Formats and adds/drops columns of a dataframe from smartfolio to conform 
        to postgres DB schema.
    
    Args:
        smartfolio (pandas dataframe): The unformatted data stored in S3 from smartfolio.
    
    Returns:
        smartfolio (pandas dataframe): Formatted dataframe that works with DB schema.
    """

    # Drop dupes as there are some overlapping dates in the smartfolio CSVs, keep latest
    smartfolio = smartfolio.drop_duplicates(subset=["SYSTEM_ID"], keep="last")

    ## Column wrangling
    smartfolio["id"] = smartfolio["SYSTEM_ID"]
    smartfolio["Banking Id"] = smartfolio["CARD_TRANS_ID"].astype("Int64")
    smartfolio["Terminal Code"] = smartfolio["METER_CODE"]

    smartfolio["invoice_id"] = smartfolio.apply(
        lambda x: get_invoice_id(x["Banking Id"], x["Terminal Code"]), axis=1
    )

    # Date/time wrangling
    smartfolio["duration_min"] = smartfolio["TOTAL_DURATION"] / 60

    smartfolio["datetime"] = smartfolio.apply(
        lambda x: postgres_datetime(x["SERVER_DATE"]), axis=1
    )
    smartfolio["start_time"] = smartfolio.apply(
        lambda x: postgres_datetime(x["METER_DATE"]), axis=1
    )
    smartfolio["end_time"] = smartfolio.apply(
        lambda x: postgres_datetime(x["END_DATE"]), axis=1
    )

    smartfolio["timestamp"] = smartfolio["datetime"]

    # Payment type column cleanup
    smartfolio.loc[smartfolio["PAYMENT_MEAN"] == "CARD_1_0", ["PAYMENT_MEAN"]] = "CARD"
    smartfolio.loc[
        smartfolio["PAYMENT_MEAN"] == "CARD_0_116", ["PAYMENT_MEAN"]
    ] = "CARD"

    # Renaming columns for schema
    smartfolio = smartfolio.rename(
        columns={
            "PAYMENT_MEAN": "payment_method",
            "Terminal Code": "meter_id",
            "AMOUNT": "amount",
            "TRANSACTION_TYPE": "transaction_type",
        }
    )

    # Data types for schema
    smartfolio["invoice_id"] = smartfolio["invoice_id"].astype(int)
    smartfolio["meter_id"] = smartfolio["meter_id"].astype(int)
    smartfolio["end_time"] = smartfolio["end_time"].replace("NaT", None)

    # Only subset of columns needed for schema
    smartfolio = smartfolio[
        [
            "id",
            "invoice_id",
            "transaction_type",
            "payment_method",
            "meter_id",
            "timestamp",
            "duration_min",
            "start_time",
            "end_time",
            "amount",
        ]
    ]

    return smartfolio


def to_postgres(smartfolio):
    """Uploads the formatted dataframe to two different postgres DBs.
        flowbird_transactions_raw - just for smartfolio aka flowbird data
        transactions - a combined parking DB which will also include data from passport
    
    Args:
        smartfolio (pandas dataframe): Formatted dataframe that works with DB schema.
    
    Returns:
        None
    """
    payload = smartfolio.to_dict(orient="records")

    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    res = client.upsert(resource="flowbird_transactions_raw", data=payload)

    smartfolio = smartfolio[
        [
            "id",
            "payment_method",
            "meter_id",
            "duration_min",
            "start_time",
            "end_time",
            "amount",
        ]
    ]

    smartfolio.loc[:, "source"] = "Parking Meters"
    payload = smartfolio.to_dict(orient="records")

    res = client.upsert(resource="transactions", data=payload)


def main(args):
    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    csv_file_list = handle_year_month_args(
        args.year, args.month, args.lastmonth, aws_s3_client
    )

    # Go through all files and combine into a dataframe
    for csv_f in csv_file_list:
        # Parse the file
        response = aws_s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_f)
        df = pd.read_csv(response.get("Body"))

        logger.debug(f"Loaded CSV File: {csv_f}")

        df = transform(df)

        to_postgres(df)


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
