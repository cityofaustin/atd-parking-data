#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 14:47:12 2021

@author: charliehenry
"""

from pypgrest import Postgrest
import pandas as pd
import boto3
import ntpath
from sodapy import Socrata
from dotenv import load_dotenv
import os

# Envrioment variables
load_dotenv("smartfolio.env")

AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
AWS_PASS = os.environ.get("AWS_PASS")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
POSTGREST_TOKEN = os.environ.get("POSTGREST_TOKEN")

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

    # Finally return the final list
    return csv_file_list


def aws_list_files(year, month):
    """
    Returns a list of email files.
    :return: object
    """
    global aws_s3_client
    response = aws_s3_client.list_objects(
        Bucket=BUCKET_NAME,
        Prefix="meters/dev/archipel_transactionspub/" + str(year) + "/" + str(month),
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


def banking_change(banking_id):
    if banking_id == "0" or banking_id == "<NA>":
        return "No Banking ID"
    if len(banking_id) >= 6:
        return banking_id
    else:
        output = "0" + banking_id
        return banking_change(output)


def invoice_num(banking_id, terminal_code):
    if banking_id == "No Banking ID":
        return "-1"
    out = terminal_code[-4:]
    out = out + "" + banking_id
    if out[0] == "0":
        return out[-1 * len(out) + 1 :]
    return out


# Get list of CSVs
csv_file_list = get_csv_list(arg_year, arg_month)

# Go through all files and combine into a dataframe
data = []
for csv_f in csv_file_list:
    # Parse the file
    if ".csv" in csv_f:
        response = aws_s3_client.get_object(Bucket=BUCKET_NAME, Key=csv_f)
        df = pd.read_csv(response.get("Body"))
        data.append(df)

        print("Loaded CSV File: '%s'" % csv_f)


smartfolio = pd.concat(data, ignore_index=True)

## Column wrangling
# Invoice ID for matching to fiserv creation
smartfolio["Banking Id"] = smartfolio["TRANSACTION_NUMBER"].astype("Int64").astype(str)

smartfolio["Terminal Code"] = smartfolio["TERMINAL_ID"].astype(str)

smartfolio["Banking Id"] = smartfolio["Banking Id"].apply(banking_change)

smartfolio["invoice_id"] = smartfolio.apply(
    lambda x: invoice_num(x["Banking Id"], x["Terminal Code"]), axis=1
)

# Date formatting
smartfolio["transaction_date"] = pd.to_datetime(
    smartfolio["TRANSACTION_DATE"],
    format="%Y-%m-%d %H:%M:%S",
    infer_datetime_format=True,
)

smartfolio["processed_date"] = pd.to_datetime(
    smartfolio["TRANSACTION_HANDLING_DATE"],
    format="%Y-%m-%d %H:%M:%S",
    infer_datetime_format=True,
)

smartfolio["transaction_date"] = smartfolio["transaction_date"].astype(str)
smartfolio["processed_date"] = smartfolio["processed_date"].astype(str)

# All transactions are credit cards in this dataset
smartfolio["transaction_type"] = "Card"

# Renaming to match schema
smartfolio = smartfolio.rename(
    columns={
        "SCHEME": "card_type",
        "TERMINAL_ID": "meter_id",
        "TRANSACTION_AMOUNT": "amount",
        "TRANSACTION_STATUS": "transaction_status",
        "REMITTANCE_STATUS": "remittance_status",
    }
)

# Data types to match schema
smartfolio["invoice_id"] = smartfolio["invoice_id"].astype(int)
smartfolio["meter_id"] = smartfolio["meter_id"].astype(int)

# Sometimes blank processed dates are present
smartfolio["processed_date"] = smartfolio["processed_date"].replace("NaT", None)

# Payload to DB
smartfolio = smartfolio[
    [
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

payload = smartfolio.to_dict(orient="records")

# Upsert to postgres
client = Postgrest(
    "http://127.0.0.1:3000",
    token=POSTGREST_TOKEN,
    headers={"Prefer": "return=representation"},
)

two = client.upsert(resource="flowbird_payments_raw", data=payload)
