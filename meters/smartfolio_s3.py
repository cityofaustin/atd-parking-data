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

# Arguments to pick which month to download from S3
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
        Bucket=BUCKET_NAME,
        Prefix="meters/prod/transaction_history/" + str(year) + "/" + str(month),
    )

    for content in response.get("Contents", []):
        yield content.get("Key")


def banking_change(banking_id):
    # Changes the bank id field to had a leading zero if it is needed
    if banking_id == "0" or banking_id == "<NA>":
        return "No Banking ID"

    if len(banking_id) >= 6:
        return banking_id

    else:
        output = "0" + banking_id
        return banking_change(output)


def invoice_num(banking_id, terminal_code):
    # Creates the Inovice ID which is a concatention of the banking ID and device ID
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

# Drop dupes as there are some overlapping dates in the smartfolio CSVs, keep latest
smartfolio = smartfolio.drop_duplicates(subset=["SYSTEM_ID"], keep="last")

## Column wrangling
smartfolio["id"] = smartfolio["SYSTEM_ID"]
smartfolio["Banking Id"] = smartfolio["CARD_TRANS_ID"].astype("Int64").astype(str)
smartfolio["Terminal Code"] = smartfolio["METER_CODE"].astype(str)

# Invoice ID for matching to fiserv creation
smartfolio["Banking Id"] = smartfolio["Banking Id"].apply(banking_change)

smartfolio["invoice_id"] = smartfolio.apply(
    lambda x: invoice_num(x["Banking Id"], x["Terminal Code"]), axis=1
)

# Date/time wrangling
smartfolio["datetime"] = pd.to_datetime(
    smartfolio["SERVER_DATE"], format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True
)

smartfolio["duration_min"] = smartfolio["TOTAL_DURATION"] / 60

smartfolio["datetime"] = smartfolio["datetime"].astype(str)

smartfolio["start_time"] = pd.to_datetime(
    smartfolio["METER_DATE"], format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True
)

smartfolio["end_time"] = pd.to_datetime(
    smartfolio["END_DATE"], format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True
)

# Convert dates back to string
smartfolio["start_time"] = smartfolio["start_time"].astype(str)
smartfolio["end_time"] = smartfolio["end_time"].astype(str)

smartfolio["timestamp"] = smartfolio["datetime"]

# Payment type column cleanup
smartfolio.loc[smartfolio["PAYMENT_MEAN"] == "CARD_1_0", ["PAYMENT_MEAN"]] = "CARD"
smartfolio.loc[smartfolio["PAYMENT_MEAN"] == "CARD_0_116", ["PAYMENT_MEAN"]] = "CARD"

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

# validated column initialzied as false
smartfolio["validated"] = False

# NA duration column must be filled with zeroes
smartfolio["duration_min"] = smartfolio["duration_min"].fillna(0)


# Upsert to database
payload = smartfolio.to_dict(orient="records")

client = Postgrest(
    "http://127.0.0.1:3000",
    token=POSTGREST_TOKEN,
    headers={"Prefer": "return=representation"},
)

two = client.upsert(resource="flowbird_transactions_raw", data=payload)
