# Standard Library imports
import os
import ntpath
import base64
import logging

# Related third party imports
import boto3
import mailparser
from dotenv import load_dotenv

import utils

# Envrioment variables

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
FSRV_EMAIL = os.getenv("FSRV_EMAIL")


# Downloads a file from s3
def download_s3_file(file_key, client):
    """
    Downloads an email file from s3
    :param file_key: the full path to the email file
    :return:
    """
    with open((get_file_name(file_key)), "wb") as f:
        client.download_fileobj(BUCKET_NAME, file_key, f)


def get_file_name(file_key):
    """
    Returns the name of an email file based on the full s3 file path
    :param file_key: the file path
    :return: string
    """
    return ntpath.basename(file_key)


def parse_email(file_key):
    """
    Returns the parsed email file object using mailparser
    :return: mailparser object
    """
    return mailparser.parse_from_file(get_file_name(file_key))


def get_email_list(client):
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings and ignores those that have already been processed
    """
    email_file_list = []
    pending_email_list = aws_list_files(client)

    # Ignores files already in the archive folder and those with zero attachments
    for email_file in pending_email_list:
        if len(get_file_name(email_file)) > 0 and "archive" not in email_file:
            email_file_list.append(email_file)

    # Remove files from list which have already been processed.
    email_file_list = [f for f in email_file_list if not f.endswith(".csv")]

    return email_file_list


def aws_list_files(client):
    """
    Returns a list of email files.
    :return: object
    """
    response = client.list_objects(Bucket=BUCKET_NAME, Prefix="emails/")

    for content in response.get("Contents", []):
        yield content.get("Key")


def format_file_name(emailObject):
    """
    Returns a file name + path for each csv file based on the email send date
    :return: string
    """
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

    return file_name


def main():
    # Initialize AWS clients

    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    s3 = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    email_file_list = get_email_list(aws_s3_client)

    logger.debug(f"Emails in inbox: {len(email_file_list)}")

    if len(email_file_list) > 0:
        for email_file in email_file_list:

            # Downloads email contents to disk
            download_s3_file(email_file, aws_s3_client)

            # Send a copy of the file to the archive folder
            s3.Object(
                BUCKET_NAME, "emails/archive/" + get_file_name(email_file)
            ).copy_from(CopySource=BUCKET_NAME + "/" + email_file)

            emailObject = parse_email(email_file)

            # email must be from Fiserv
            if (
                len(emailObject.attachments) > 0
                and emailObject.headers["Return-Path"] == FSRV_EMAIL
            ):

                logger.debug(f"Loaded Email File: {email_file}")

                # Create a file name and path for the email
                file_name = format_file_name(emailObject)

                # Decoding the email attachments
                message_bytes = base64.b64decode(emailObject.attachments[0]["payload"])
                message_decoded = message_bytes.decode("utf-16")

                # Uploading CSV to S3
                upload = s3.Object(BUCKET_NAME, file_name)
                upload.put(Body=message_decoded)

                logger.debug(f"Uploaded file: {file_name}")

                # Removes the file from processed folderpp
                s3.Object(BUCKET_NAME, email_file).delete()

    else:
        logger.debug(f"Zero emails in inbox, nothing happened.")


logger = utils.get_logger(__file__, level=logging.DEBUG)

main()
