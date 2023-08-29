# Standard Library imports
import os
import ntpath
import base64
import logging

# Related third party imports
import boto3
import mailparser
import pandas as pd
import pyzipper

import utils
from io import StringIO
from io import BytesIO

# Envrioment variables

AWS_ACCESS_ID = os.getenv("AWS_ACCESS_ID")
AWS_PASS = os.getenv("AWS_PASS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
FSRV_EMAIL = os.getenv("FSRV_EMAIL")
ENCRYPTION_KEY = os.getenv("FSRV_ENCRYPTION")


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


def get_email_list(s3):
    """
    Returns an array of files parsed into an actual array (as opposed to an object)
    :return: array of strings of emails in our inbox
    """
    email_file_list = []

    my_bucket = s3.Bucket(BUCKET_NAME)

    for object_summary in my_bucket.objects.filter(Prefix="emails/new"):
        email_file_list.append(object_summary.key)

    return email_file_list


def format_file_name(emailObject):
    """
    Returns a file name + path for each csv file based on the email send date
    :return: string
    """

    file_name = (
        "emails/current_processed/"
        + str(emailObject.date.year)
        + "/"
        + str(emailObject.date.month)
        + "/"
        + emailObject.attachments[0]["filename"]
        .replace(" ", "-")
        .replace(".zip", ".csv")
    )

    return file_name


def decode_file_contents(email_data, fname):
    """
    Decodes the password protected AES256 encrypted zip file from Fiserv with the password we set.

    Args:
        email_data: Raw base64 payload from the email attachment
        fname: the name of the CSV file to extract from the email attachment

    Returns:
        df: a pandas dataframe of the email CSV

    """
    zip_data = BytesIO(base64.b64decode(email_data))
    with pyzipper.AESZipFile(
        zip_data, "r", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES
    ) as extracted_zip:
        with extracted_zip.open(fname, pwd=str.encode(ENCRYPTION_KEY)) as csv_file:
            df = pd.read_csv(csv_file)
    return df


def df_to_s3(df, resource, filename):
    """
    Send pandas dataframe to an S3 bucket as a CSV
    h/t https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python
    Parameters
    ----------
    df : Pandas Dataframe
    resource : boto3 s3 resource
    filename : String of the file that will be created in the S3 bucket ex:
    """
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    resource.Object(BUCKET_NAME, filename).put(Body=csv_buffer.getvalue())


def main():
    # Initialize AWS clients
    aws_s3_client = boto3.client(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    s3 = boto3.resource(
        "s3", aws_access_key_id=AWS_ACCESS_ID, aws_secret_access_key=AWS_PASS,
    )

    email_file_list = get_email_list(s3)

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
                and emailObject.headers["From"] == FSRV_EMAIL
            ):

                logger.debug(f"Loaded Email File: {email_file}")

                # Create a file name and path for the email
                file_name = format_file_name(emailObject)
                attachment_name = emailObject.attachments[0]["filename"][:-3]
                attachment_name = f"{attachment_name}csv"

                # Parse attachment contents
                df = decode_file_contents(
                    emailObject.attachments[0]["payload"], attachment_name
                )

                # Uploading CSV to S3
                df_to_s3(df, s3, file_name)
                logger.debug(f"Uploaded file: {file_name}")

                # Removes the file from processed folder
                s3.Object(BUCKET_NAME, email_file).delete()

    else:
        logger.debug(f"Zero emails in inbox, nothing happened.")


logger = utils.get_logger(__file__, level=logging.DEBUG)

main()
