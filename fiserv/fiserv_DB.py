#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  1 14:07:43 2021

@author: charliehenry
"""

import boto3
import pandas as pd
from sodapy import Socrata
import ntpath
from pypgrest import Postgrest
from dotenv import load_dotenv
import os

# Envrioment variables
load_dotenv("fiserv.env")

AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
AWS_PASS = os.environ.get("AWS_PASS")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
POSTGREST_TOKEN = os.environ.get("POSTGREST_TOKEN")

# Arguments select which month folder in S3 to download
arg_year = 2021
arg_month = 12

aws_s3_client = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
)

# Downloads a file from s3
def download_s3_file(file_key):
    """
    Downloads an email file from s3
    :param file_key: the full path to the email file
    :return:
    """
    with open((get_file_name(file_key)), "wb") as f:
        aws_s3_client.download_fileobj(BUCKET_NAME, file_key, f)


def get_file_name(file_key):
    """
    Returns the name of an email file based on the full s3 file path
    :param file_key: the file path
    :return: string
    """
    return ntpath.basename(file_key)


def get_csv_list(year, month):
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings
    """
    csv_file_list = []
    pending_csv_list = aws_list_files(year, month)
    for csv_file in pending_csv_list:
        csv_file_list.append(csv_file)

    # Remove the first item, it is not needed
    # since it is just the name of the folder
    csv_file_list.pop(0)

    # Finally return the final list
    return csv_file_list


def aws_list_files(year, month):
    """
    Returns a list of email files.
    :return: object
    """
    global aws_s3_client
    response = aws_s3_client.list_objects(
        Bucket=BUCKET_NAME, Prefix="emails/processed/" + str(year) + "/" + str(month),
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


csv_file_list = get_csv_list(arg_year, arg_month)


# Access the files from S3 and place them into a dataframe
data = []
for csv_f in csv_file_list:
    # Parse the file
    if ".csv" in csv_f:
        response = aws_s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_f)
        df = pd.read_csv(response.get("Body"), sep="\t")
        data.append(df)

        print("Loaded CSV File: '%s'" % csv_f)

# Creates dataframe from batch of CSVs
fiserv_df = pd.concat(data, ignore_index=True)

# Subset of columns needed
fiserv_df = fiserv_df[
    [
        "Invoice Number",
        "Transaction Date",
        "DBA Name",
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
        "Transaction Date": "transaction_date",
        "DBA Name": "transaction_type",
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

# Initialize validiated column as false for now
fiserv_df["validated"] = False

# funded date is assumed to be transaction_date
fiserv_df["funded_date"] = fiserv_df["transaction_date"]

# formatting before upsert
fiserv_df["invoice_id"] = fiserv_df["invoice_id"].astype("int64")
fiserv_df["batch_number"] = fiserv_df["batch_number"].astype("int64")
fiserv_df["batch_sequence_number"] = fiserv_df["batch_sequence_number"].astype("int64")
fiserv_df["meter_id"] = fiserv_df["meter_id"].astype("int64")


payload = fiserv_df.to_dict(orient="records")

# Connect to local DB
client = Postgrest(
    "http://127.0.0.1:3000",
    token=POSTGREST_TOKEN,
    headers={"Prefer": "return=representation"},
)

# Upsert to postgres DB
two = client.upsert(resource="fiserv_reports_raw", data=payload)
