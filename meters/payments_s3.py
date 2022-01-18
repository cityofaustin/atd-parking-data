import ntpath
import os
import logging
import argparse
from datetime import datetime


from pypgrest import Postgrest
import pandas as pd
import boto3
from dotenv import load_dotenv

import utils

# Envrioment variables
load_dotenv("smartfolio.env")

AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
AWS_PASS = os.environ.get("AWS_PASS")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
POSTGREST_TOKEN = os.environ.get("POSTGREST_TOKEN")


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

    # Finally return the final list
    return csv_file_list


def aws_list_files(year, month, client):
    """
    Returns a list of email files.
    :return: object
    """
    response = client.list_objects(
        Bucket=BUCKET_NAME,
        Prefix="meters/dev/archipel_transactionspub/" + str(year) + "/" + str(month),
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


def match_field_creation(card_num, invoice_id):
    """
    Returns a field for matching between Fiserv and Smartfolio
    It is defined as a concatenation of the Credit Card number and the invoice ID
    
    :return: String
    """
    return card_num + "-" + str(invoice_id)


def transform(smartfolio):
    """Formats and adds/drops columns of a dataframe from smartfolio to conform 
        to postgres DB schema.
    
    Args:
        smartfolio (pandas dataframe): The unformatted data stored in S3 from smartfolio.
    
    Returns:
        smartfolio (pandas dataframe): Formatted dataframe that works with DB schema.
    """

    ## Column wrangling
    # Invoice ID for matching to fiserv creation
    smartfolio["Banking Id"] = smartfolio["TRANSACTION_NUMBER"].astype("Int64")

    smartfolio["Terminal Code"] = smartfolio["TERMINAL_ID"]

    smartfolio["invoice_id"] = smartfolio.apply(
        lambda x: get_invoice_id(x["Banking Id"], x["Terminal Code"]), axis=1
    )

    # Date formatting
    smartfolio["transaction_date"] = smartfolio.apply(
        lambda x: postgres_datetime(x["TRANSACTION_DATE"]), axis=1
    )
    smartfolio["processed_date"] = smartfolio.apply(
        lambda x: postgres_datetime(x["TRANSACTION_HANDLING_DATE"]), axis=1
    )

    # All transactions are credit cards in this dataset
    smartfolio["transaction_type"] = "Card"

    # Renaming to match schema
    smartfolio = smartfolio.rename(
        columns={
            "MONETRA_ID": "id",
            "PAN_HIDDEN": "match_field",
            "SCHEME": "card_type",
            "TERMINAL_ID": "meter_id",
            "TRANSACTION_AMOUNT": "amount",
            "TRANSACTION_STATUS": "transaction_status",
            "REMITTANCE_STATUS": "remittance_status",
        }
    )

    # Drops "INCOMPLETE"/"UNSUCCESSFUL" transactions which we don't need.
    smartfolio = smartfolio.dropna(subset=["id"])
    smartfolio = smartfolio[smartfolio["transaction_status"] == "COMPLETED"]

    # Data types to match schema
    smartfolio["invoice_id"] = smartfolio["invoice_id"].astype(int)
    smartfolio["meter_id"] = smartfolio["meter_id"].astype(int)
    smartfolio["id"] = smartfolio["id"].astype(int)

    # Sometimes blank processed dates are present
    smartfolio["processed_date"] = smartfolio["processed_date"].replace("NaT", None)

    smartfolio["match_field"] = smartfolio.apply(
        lambda x: match_field_creation(x["match_field"], x["invoice_id"]), axis=1
    )

    # Payload to DB
    smartfolio = smartfolio[
        [
            "id",
            "match_field",
            "invoice_id",
            "card_type",
            "meter_id",
            "transaction_type",
            "transaction_date",
            "transaction_status",
            "remittance_status",
            "processed_date",
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
    # Upsert to database
    payload = smartfolio.to_dict(orient="records")

    client = Postgrest(
        "http://127.0.0.1:3000",
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )
    try:
        res = client.upsert(resource="flowbird_payments_raw", data=payload)
    except:
        logger.debug(client.res.text)
        raise


def main(args):
    # Arguments to pick which month to download from S3
    year = args.year
    month = args.month

    # If args are missing, default to current month and/or year
    if not year:
        year = datetime.now().year

    if not month:
        month = datetime.now().month

    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    # Get list of CSVs
    csv_file_list = get_csv_list(year, month, aws_s3_client)

    # Go through all files and combine into a dataframe
    csv_file_list = [f for f in csv_file_list if f.endswith(".csv")]

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

args = parser.parse_args()

logger = utils.get_logger(__file__, level=logging.DEBUG)

main(args)
