import csv
from io import StringIO
import requests
from datetime import datetime, timezone, timedelta
import argparse
import logging
import os
import hashlib

import pandas as pd
from pypgrest import Postgrest

import utils

## Credentials for flowbird HUB
HUB_USER = os.getenv("HUB_USER")
HUB_PASS = os.getenv("HUB_PASS")
HUB_URL = os.getenv("HUB_URL")
API_URL = os.environ.get("API_URL")

## Random string for hashing to create IDs
SALT = os.environ.get("SALT")

## Postgrest credentials
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")

DATE_FORMAT_INPUT = "%Y-%m-%d"


def handle_date_args(start_string, end_string):
    """Parse or set default start and end dates from CLI args.

    Args:
        start_string (string): Date (in UTC) of earliest records to be fetched (YYYY-MM-DD).
            Defaults to yesterday.
        end_string (string): Date (in UTC) of most recent records to be fetched (YYYY-MM-DD).
            Defaults to today.

    Returns:
        list: The start date and end date as python datetime objects
    """
    if start_string:
        # parse CLI arg date
        start_date = datetime.strptime(start_string, DATE_FORMAT_INPUT).replace(
            tzinfo=timezone.utc
        )
    else:
        # create yesterday's date
        start_date = datetime.now(timezone.utc) - timedelta(days=1)

    if end_string:
        # parse CLI arg date
        end_date = datetime.strptime(end_string, DATE_FORMAT_INPUT).replace(
            tzinfo=timezone.utc
        )
    else:
        # create today's date
        end_date = datetime.now(timezone.utc)

    return (
        start_date.strftime(DATE_FORMAT_INPUT),
        end_date.strftime(DATE_FORMAT_INPUT),
    )


def get_data(session, report, start, end):
    """
    Grabs data from the Flowbird HUB API and returns a transformed dataframe

    Parameters
    ----------
    session : Sessions object
        Session logged into the flowbird API.
    report : String
        Which of the reports to download, current options: tvm, web, mobile
    start : String
        Start date argument formatted to match the API.
    end : String
        End date argument formatted to match the API.

    Returns
    -------
    df : Pandas Dataframe
        Transformed response from the API ready to be sent to postgres.

    """
    start_date = f"{start} 12:00:00 AM"
    end_date = f"{end} 12:00:00 AM"

    params = {
        "filter[generated_after]": start_date,
        "filter[generated_before]": end_date,
    }

    logger.debug(f"Getting data for {report} from {start} to {end}")

    try:
        res = session.get(f"{API_URL}{report}", params=params,)
        csv_io = StringIO(res.text)
        reader = csv.DictReader(csv_io)
        data = [row for row in reader]
        df = pd.DataFrame(data)

        if not df.empty:
            logger.debug(f"{len(df)} records retrived of {report} report")
            if report == "tvm":
                df["id"] = df.apply(
                    lambda x: id_creation(
                        x["Transaction Date"], x["Total Charge"], x["Machine ID"]
                    ),
                    axis=1,
                )

                df = transform_tvm(df)
            else:
                df["id"] = df.apply(
                    lambda x: id_creation(
                        x["Transaction Date"], x["Total Charge"], x["Patron Name"]
                    ),
                    axis=1,
                )
                df = transform_web_mobile(df, report)
        else:
            logger.debug(f"No Data found for {report} from {start} to {end}")

        return df

    except:
        res.raise_for_status()
        raise


def id_creation(field1, field2, field3):
    """
    Creates a unique ID for each row in the database based on three provided fields
    
    Parameters
    ----------
    field1-3 : int or String
        The three fields that are combined to create a unique ID for
            each row in the database.

    Returns
    -------
    h.hexdigest(): String
        A hashed stirng of characters using MD5 that is salted

    """

    if SALT == None:
        # No writing to the database without salt as it'll break things
        raise Exception("No salt found in enviroment file")

    concat_id = str(field1) + str(field2) + str(field3) + SALT

    h = hashlib.md5(concat_id.encode())

    return h.hexdigest()


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


def format_currency(df, field_names):
    """
    Changes string currency fields ($0.00) in a Dataframe to a 
        floating point decimal (0.00). 

    Parameters
    ----------
    df : Pandas Dataframe
        The dataframe with columns that need to be modified.
    field_names : List
        A list of the fields that should be modified.

    Returns
    -------
    df : Pandas Dataframe
        Dataframe with the fields modified.

    """

    for field in field_names:
        df[field] = df[field].str.replace("$", "", regex=True)
        df[field] = df[field].replace(r"^\s*$", "0", regex=True)
        df[field] = df[field].astype(float)

    return df


def transform_tvm(df):
    """
    Transforms the tvm report from flowbird HUB to a format that works 
        with our DB schema.

    Parameters
    ----------
    df : Pandas Dataframe
        The data from the flowbird HUB API.

    Returns
    -------
    df : Pandas Dataframe
        Data modified to work with the postgres schema.

    """
    df["source"] = "Flowbird HUB - TVM"

    df["transaction_date"] = df.apply(
        lambda x: postgres_datetime(x["Transaction Date"]), axis=1
    )

    df = df.rename(
        columns={
            "Payment Mean": "payment_method",
            "Machine ID": "meter_id",
            "Machine Name": "meter_name",
            "Total Charge": "amount_charged",
            "Amount Paid": "amount_paid",
            "Refunded": "amount_refuned",
            "Returned": "amount_returned",
            "Voucher Amount": "amount_voucher",
            "Status": "status",
        }
    )

    format_currency(
        df,
        [
            "amount_charged",
            "amount_paid",
            "amount_refuned",
            "amount_returned",
            "amount_voucher",
        ],
    )

    df["payment_method"] = df["payment_method"].replace(r"^\s*$", "Voucher", regex=True)

    df = df.drop(
        columns=["Transaction Date", "Amount Due", "Payment Detail", "Payment Amount"]
    )

    return df


def transform_web_mobile(df, report):
    """
    Transforms the web or mobile reports from flowbird HUB to a format that works 
        with our DB schema.

    Parameters
    ----------
    df : Pandas Dataframe
        The data from the flowbird HUB API.
        
    report: String
        Which report, mobile or web

    Returns
    -------
    df : Pandas Dataframe
        Data modified to work with the postgres schema.

    """
    if report == "web":
        df["source"] = "Flowbird HUB - Web"
    elif report == "mobile":
        df["source"] = "Flowbird HUB - Mobile"
    else:
        return df

    df["transaction_date"] = df.apply(
        lambda x: postgres_datetime(x["Transaction Date"]), axis=1
    )

    df = df.rename(
        columns={
            "Payment Mean": "payment_method",
            "Total Charge": "amount_charged",
            "Amount Paid": "amount_paid",
            "Refunded": "amount_refuned",
            "Returned": "amount_returned",
            "Voucher Amount": "amount_voucher",
            "Status": "status",
        }
    )

    format_currency(
        df,
        [
            "amount_charged",
            "amount_paid",
            "amount_refuned",
            "amount_returned",
            "amount_voucher",
        ],
    )

    df["payment_method"] = df["payment_method"].replace(r"^\s*$", "Voucher", regex=True)

    df = df.drop(
        columns=[
            "Transaction Date",
            "Amount Due",
            "Payment Detail",
            "Payment Amount",
            "Patron Name",
            "Type",
        ]
    )

    return df


def to_postgres(df, client):
    """Uploads the formatted dataframe to a postgres DB.
    Args:
        df (pandas dataframe): Formatted dataframe that works with DB schema.
    
    Returns:
        None
    """
    # Upsert to database
    payload = df.to_dict(orient="records")

    try:
        res = client.upsert(resource="flowbird_hub_transactions_raw", data=payload)
    except:
        logger.debug(client.res.text)
        raise


def main(args):

    start, end = handle_date_args(args.start, args.end)

    session = requests.Session()

    login_url = HUB_URL
    email = HUB_USER
    password = HUB_PASS

    res = session.post(login_url, json={"email": email, "password": password})

    tvm = get_data(session, "tvm", start, end)
    mobile = get_data(session, "mobile", start, end)
    web = get_data(session, "web", start, end)

    client = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    to_postgres(tvm, client)
    to_postgres(web, client)
    to_postgres(mobile, client)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--start",
        type=str,
        help=f"Date (in UTC) of earliest records to be fetched (YYYY-MM-DD). Defaults to yesterday",
    )

    parser.add_argument(
        "--end",
        type=str,
        help=f"Date (in UTC) of the most recent records to be fetched (YYYY-MM-DD). Defaults to today",
    )

    args = parser.parse_args()

    logger = utils.get_logger(__file__, level=logging.DEBUG)

    main(args)
