#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  3 14:33:25 2021

@author: charliehenry
"""
from sodapy import Socrata
from pypgrest import Postgrest
from dotenv import load_dotenv
import os

# Envrioment variables
load_dotenv("socrata.env")

# Credentials
POSTGREST_TOKEN = os.environ.get("POSTGREST_TOKEN")
SO_WEB = os.environ.get("SO_WEB")
SO_TOKEN = os.environ.get("SO_TOKEN")
SO_USER = os.environ.get("SO_USER")
SO_PASS = os.environ.get("SO_PASS")

# Socrata dataset IDs
FISERV_DATASET = os.environ.get("FISERV_DATASET")
TRANSACTIONS_DATASET = os.environ.get("TRANSACTIONS_DATASET")
PAYMENTS_DATASET = os.environ.get("PAYMENTS_DATASET")


pstgrs = Postgrest(
    "http://127.0.0.1:3000",
    token=POSTGREST_TOKEN,
    headers={"Prefer": "return=representation"},
)

client = Socrata(SO_WEB, SO_TOKEN, username=SO_USER, password=SO_PASS, timeout=500,)


## Fiserv reports upsert to socrata
params = {"select": "*", "order": "invoice_id"}

response = pstgrs.select(resource="fiserv_reports_raw", params=params)

client.upsert(FISERV_DATASET, response)


## Flowbird transactions upsert to socrata
params = {"select": "*", "order": "id"}

response = pstgrs.select(resource="flowbird_transactions_raw", params=params)

client.upsert(TRANSACTIONS_DATASET, response)


## Flowbird payment upsert to socrata
params = {"select": "*", "order": "invoice_id"}

response = pstgrs.select(resource="flowbird_payments_raw", params=params)

client.upsert(PAYMENTS_DATASET, response)
