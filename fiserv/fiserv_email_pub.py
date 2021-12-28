#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 29 15:27:52 2021

@author: charliehenry
"""

import boto3
import mailparser
import ntpath
import base64
from dotenv import load_dotenv
import os

# Envrioment variables
load_dotenv("fiserv.env")

AWS_ACCESS_ID = os.environ.get("AWS_ACCESS_ID")
AWS_PASS = os.environ.get("AWS_PASS")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
FSRV_EMAIL = os.environ.get("FSRV_EMAIL")

# Initialize AWS clients

aws_s3_client = boto3.client(
    "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
)

s3 = boto3.resource(
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


# Parses an email from file
def parse_email(file_key):
    return mailparser.parse_from_file(get_file_name(file_key))


def get_email_list():
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings and ignores those that have already been processed
    """
    email_file_list = []
    pending_email_list = aws_list_files()

    for email_file in pending_email_list:
        if len(get_file_name(email_file)) > 0 and "archive" not in email_file:
            email_file_list.append(email_file)

    # Finally return the final list
    return email_file_list


def aws_list_files():
    """
    Returns a list of email files.
    :return: object
    """
    global aws_s3_client
    response = aws_s3_client.list_objects(Bucket=BUCKET_NAME, Prefix="emails/")

    for content in response.get("Contents", []):
        yield content.get("Key")


email_file_list = get_email_list()

# Downloads the content of each email to disk
for email in email_file_list:
    download_s3_file(email)


# Parses the email files to save the attachment csv locally then to S3
for email_file in email_file_list:

    if ".csv" not in email_file and "archive" not in email_file:
        emailObject = parse_email(email_file)

        # Send a copy of the file to the archive folder
        s3.Object(BUCKET_NAME, "emails/archive/" + get_file_name(email_file)).copy_from(
            CopySource=BUCKET_NAME + "/" + email_file
        )

        # email must be from Fiserv
        if (
            len(emailObject.attachments) > 0
            and emailObject.headers["Return-Path"] == FSRV_EMAIL
        ):

            print("Loaded Email File: '%s'" % email_file)

            date_email = (
                str(emailObject.date.month)
                + "-"
                + str(emailObject.date.day)
                + "-"
                + emailObject.attachments[0]["filename"].replace(" ", "-")
            )

            file_name = (
                "emails/processed/"
                + str(emailObject.date.year)
                + "/"
                + str(emailObject.date.month)
                + "/"
                + date_email
            )

            print(file_name)

            message_bytes = base64.b64decode(emailObject.attachments[0]["payload"])
            message_decoded = message_bytes.decode("utf-16")

            object = s3.Object(BUCKET_NAME, file_name)
            object.put(Body=message_decoded)

        # Removes the file from processed folder
        s3.Object(BUCKET_NAME, email_file).delete()
