# Standard Library i`ports
import os
import argparse
from datetime import datetime, timezone
import logging

# Related third-party imports
from pypgrest import Postgrest
from dotenv import load_dotenv
import pandas as pd

import utils

# Envrioment variables
DATE_FORMAT_HUMANS = "%Y-%m-%d"

# Credentials
POSTGREST_TOKEN = os.getenv("POSTGREST_TOKEN")
POSTGREST_ENDPOINT = os.getenv("POSTGREST_ENDPOINT")


def handle_date_args(start_string, end_string):
    """Parse or set default start and end dates from CLI args.

    Args:
        start_string (string): Date (in UTC) of earliest records to be fetched (YYYY-MM-DD).
            Defaults to 2022-01-01.
        end_string (string): Date (in UTC) of most recent records to be fetched (YYYY-MM-DD).
            Defaults to today.
    
    Returns:
        list: The start date and end date as python datetime objects
    """
    if start_string:
        # parse CLI arg date
        start_date = datetime.strptime(start_string, DATE_FORMAT_HUMANS).replace(
            tzinfo=timezone.utc
        )
    else:
        # if not given set to January 1st, 2022
        start_date = datetime(2022, 1, 1, 0, 0, 0, 0, timezone.utc)

    if end_string:
        # parse CLI arg date
        end_date = datetime.strptime(end_string, DATE_FORMAT_HUMANS).replace(
            tzinfo=timezone.utc
        )
    else:
        # create today's date
        end_date = datetime.now(timezone.utc)

    return start_date, end_date


def get_fiserv(pstgrs):
    """
    Asks postgres for the fiserv data that currently do not have a matching record in the payments dataset.
    Parameters
    ----------
    pstgrs : Postgrest client object

    Returns
    -------
    fiserv : Pandas Dataframe
        The rows in the fiserv database that do not currently have a match with flowbird.

    """
    params = {
        "select": "id,match_field,transaction_date,flowbird_id",
        "order": "id",
        "flowbird_id": "is.null",
    }

    fiserv = pstgrs.select(resource="fiserv_reports_raw", params=params)

    fiserv = pd.DataFrame(fiserv)
    return fiserv


def get_payments(pstgrs, start, end):
    """
    Asks postgres for the payments data based on CLI arguments (if given)
    Parameters
    ----------
    pstgrs : Postgrest client object
    start : String
        Start date of the updated_at field to search for potential matches.
    end : String
        End date of the updated_at field to search for potential matches.

    Returns
    -------
    payments : Pandas Dataframe
        The rows of the payments database that are between the start/end dates.

    """
    params = {
        "select": "id,match_field,transaction_date,updated_at",
        "order": "id",
        "and": f"(updated_at.lte.{end},updated_at.gte.{start})",
    }
    payments = pstgrs.select(resource="flowbird_payments_raw", params=params)

    payments = pd.DataFrame(payments)

    return payments


def datetime_handling(df):
    """
    Changes transacation_date column from type String to datetime and sorts by that column
    Parameters
    ----------
    df : Pandas Dataframe
        The dataframe that needs the transaction_date field changed from a string to a datetime datatype.

    Returns
    -------
    df : Pandas Dataframe
        The input dataframe sorted by the transcation_date with the datetime datatype.

    """
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"], format="%Y-%m-%d %H:%M:%S", infer_datetime_format=True,
    )
    df = df.sort_values(by=["transaction_date"])

    return df


def to_postgres(output, pstgrs):
    """
    This function cleans up the merged dataframe and then upserts the matched data back to the database
    Parameters
    ----------
    output : Pandas Dataframe
        The merged dataframe that is going to be upserted to the fiserv database.
    pstgrs : Postgrest client object

    Returns
    -------
    None.

    """
    output = output[["id_x", "id_y"]]

    output = output.rename(columns={"id_x": "id", "id_y": "flowbird_id"})

    output = output[["id", "flowbird_id"]]

    output = output.dropna(subset=["flowbird_id"])

    output["flowbird_id"] = output["flowbird_id"].astype(int)

    payload = output.to_dict(orient="records")

    try:
        pstgrs.upsert(resource="fiserv_reports_raw", data=payload)
    except:
        logger.debug(pstgrs.res.text)
        raise


def main(args):
    # Define postgrest client object with credentials
    pstgrs = Postgrest(
        POSTGREST_ENDPOINT,
        token=POSTGREST_TOKEN,
        headers={"Prefer": "return=representation"},
    )

    # format date arugments
    start_date, end_date = handle_date_args(args.start, args.end)

    # Get data from postgres database
    fiserv = get_fiserv(pstgrs)
    payments = get_payments(pstgrs, start_date, end_date)

    # Handling datatype for the transaction_date column (postgres returns a str)
    fiserv = datetime_handling(fiserv)
    payments = datetime_handling(payments)

    # Merge the two datasets first based on the match_field column.
    # If there are multiple matches (dupes),
    # then match based on the closest transaction_date.
    output = pd.merge_asof(
        left=fiserv,
        right=payments,
        by="match_field",
        on="transaction_date",
        direction="nearest",
    )
    # Clean up the output table and send it back to postgres
    to_postgres(output, pstgrs)


# CLI arguments definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--start",
    type=str,
    help=f"Date (in UTC) of earliest records to be searched for matches (YYYY-MM-DD). Defaults to 2022-01-01.",
)

parser.add_argument(
    "--end",
    type=str,
    help=f"Date (in UTC) of the most recent records to be searched for matches (YYYY-MM-DD). Defaults to today",
)

args = parser.parse_args()

logger = utils.get_logger(__file__, level=logging.DEBUG)

main(args)
