# Standard library imports
import os
import ntpath
import logging
import argparse
from datetime import datetime


# Related third-party imports
import boto3
import pandas as pd
from pypgrest import Postgrest
from dotenv import load_dotenv

import utils

# Envrioment variables
load_dotenv("fiserv.env")

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
        Bucket=BUCKET_NAME, Prefix="emails/processed/" + str(year) + "/" + str(month),
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


def match_field_creation(card_num, invoice_id):
    """
    Returns a field for matching between Fiserv and Smartfolio
    It is defined as a concatenation of the Credit Card number and the invoice ID
    
    :return: String
    """
    card_num = card_num.replace("*", "x")
    return card_num + "-" + str(invoice_id)


def id_field_creation(invoice_id, batch_number, sequence_number):
    """
    Returns a field for matching between Fiserv and Smartfolio
    It is defined as a concatenation of the Credit Card number and the invoice ID
    
    :return: int
    """

    return int(str(batch_number) + str(sequence_number) + str(invoice_id))


def transform(fiserv_df):
    """
    Formats and adds columns to a dataframe of Fiserv data for upload to postgres DB
    Args: dataframe of data from fiserv report csv
    Returns: formatted dataframe to conform to postgres schema
    """
    fiserv_df = fiserv_df[
        [
            "Invoice Number",
            "Cardholder Number",
            "Transaction Date",
            "Transaction Type",
            "Terminal ID",
            "Batch Number",
            "Batch Sequence Number",
            "Submit Date",
            "Funded Date",
            "Processed Transaction Amount",
            "Transaction Status",
            "Location ID",
        ]
    ]

    # Renaming columns to match schema
    fiserv_df = fiserv_df.rename(
        columns={
            "Invoice Number": "invoice_id",
            "Cardholder Number": "match_field",
            "Transaction Date": "transaction_date",
            "Transaction Type": "transaction_type",
            "Terminal ID": "meter_id",
            "Batch Number": "batch_number",
            "Batch Sequence Number": "batch_sequence_number",
            "Submit Date": "submit_date",
            "Funded Date": "funded_date",
            "Processed Transaction Amount": "amount",
            "Transaction Status": "transaction_status",
            "Location ID": "account",
        }
    )

    # Account number field is only the last three digits of the account number
    fiserv_df["account"] = fiserv_df["account"].astype(str).str[-3:].astype(int)

    params = {"select": "invoice_id", "order": "invoice_id"}

    # funded date is assumed to be transaction_date
    fiserv_df["funded_date"] = fiserv_df["transaction_date"]

    # formatting before upsert
    fiserv_df["invoice_id"] = fiserv_df["invoice_id"].astype("int64")
    fiserv_df["batch_number"] = fiserv_df["batch_number"].astype("int64")
    fiserv_df["batch_sequence_number"] = fiserv_df["batch_sequence_number"].astype(
        "int64"
    )
    fiserv_df["meter_id"] = fiserv_df["meter_id"].astype("int64")

    # Field for matching between Fiserv and Flowbird

    fiserv_df["match_field"] = fiserv_df.apply(
        lambda x: match_field_creation(x["match_field"], x["invoice_id"]), axis=1
    )

    fiserv_df["id"] = fiserv_df.apply(
        lambda x: id_field_creation(
            x["invoice_id"], x["batch_number"], x["batch_sequence_number"]
        ),
        axis=1,
    )
    # Drop dupes, sometimes there are duplicate records emailed
    fiserv_df = fiserv_df.drop_duplicates(subset=["invoice_id"], keep="first")

    return fiserv_df


def to_postgres(fiserv_df):
    """
    Upserts fiserv data to local postgres DB
    Args: Formatted dataframe from transform function
    Returns: None.
    """
    payload = fiserv_df.to_dict(orient="records")

    # Connect to local DB
    client = Postgrest(
        "http://127.0.0.1:3000",
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    # Upsert to postgres DB
    try:
        client.upsert(resource="fiserv_reports_raw", data=payload)
    except:
        logger.debug(client.res.text)
        raise


def main(args):

    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    # Arguments to pick which month to download from S3
    year = args.year
    month = args.month

    # If args are missing, default to current month and/or year
    if not year:
        year = datetime.now().year

    if not month:
        month = datetime.now().month

    csv_file_list = get_csv_list(year, month, aws_s3_client)

    csv_file_list = [f for f in csv_file_list if f.endswith(".csv")]

    # Access the files from S3 and place them into a dataframe
    for csv_f in csv_file_list:
        # Parse the file
        response = aws_s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_f)
        df = pd.read_csv(response.get("Body"), sep="\t")

        logger.debug(f"Loaded CSV File: {csv_f}")
        # Ignore the emails which send a CSV with only column headers
        # This happens with the "Contactless-Detail" reports for some reason
        if not df.empty:
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
