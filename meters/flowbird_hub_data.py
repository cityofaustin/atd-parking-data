import csv
from io import StringIO
import requests
from datetime import datetime, timezone, timedelta
import argparse
import logging
import os

import pandas as pd
from dotenv import load_dotenv
from pypgrest import Postgrest

import utils

# Envrioment variables
load_dotenv("hub.env")

HUB_USER = os.environ.get("HUB_USER")
HUB_PASS = os.environ.get("HUB_PASS")
HUB_URL = os.environ.get("HUB_URL")
API_URL = os.environ.get("API_URL")

POSTGREST_TOKEN = os.environ.get("LOCAL_TOKEN")

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

    return start_date, end_date


def get_data(session, report, start, end):
    start_date = f"{start} 12:00:00 AM"
    end_date = f"{end} 12:00:00 AM"

    params = {
        "filter[generated_after]": start_date,
        "filter[generated_before]": end_date,
    }

    try:
        res = session.get(f"{API_URL}{report}", params=params,)
        csv_io = StringIO(res.text)
        reader = csv.DictReader(csv_io)
        data = [row for row in reader]
        df = pd.DataFrame(data)

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

        return df

    except:
        res.raise_for_status()
        raise


def id_creation(field1, field2, field3):
    concat = str(field1) + str(field2) + str(field3)

    return hash(concat)


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

    for field in field_names:
        df[field] = df[field].str.replace("$", "", regex=True)
        df[field] = df[field].replace(r"^\s*$", "0", regex=True)
        df[field] = df[field].astype(float)

    return df


def transform_tvm(df):
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
    if report == "web":
        df["source"] = "Flowbird HUB - Web"
    else:
        df["source"] = "Flowbird HUB - Mobile"

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
    """Uploads the formatted dataframe to two different postgres DBs.
        flowbird_transactions_raw - just for smartfolio aka flowbird data
        transactions - a combined parking DB which will also include data from passport
    
    Args:
        smartfolio (pandas dataframe): Formatted dataframe that works with DB schema.
    
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
    session = requests.Session()

    login_url = HUB_URL
    email = HUB_USER
    password = HUB_PASS

    res = session.post(login_url, json={"email": email, "password": password})

    tvm = get_data(session, "tvm", "2021-12-01", "2022-01-01")
    mobile = get_data(session, "mobile", "2021-12-01", "2022-03-01")
    web = get_data(session, "web", "2021-12-01", "2022-03-01")

    client = Postgrest(
        "http://127.0.0.1:3000",
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    to_postgres(tvm, client)
    to_postgres(web, client)
    to_postgres(mobile, client)

    return tvm, mobile, web


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

    tvm, mobile, web = main(args)
