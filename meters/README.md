Scripts for processing parking meter transactions from the Flowbird DR-Direct tool.

## txn_history.py

Fetch Flowbird meter transactions and load to S3. This processes the "txn_history" report from the Flowbird Dr-Direct tool.

One file is generated per day in the provided range and uploaded to S3 at: `<bucket-name>/meters/<environment>/transaction_history/year/month/<query-start-date>.csv`

### Environment variables

- `USER`: Dr-direct username
- `PASSWORD`: Dr-Direct password
- `ENDPOINT`: Dr-Direct service endpoint
- `BUCKET`: S3 bucket name
- `AWS_ACCESS_KEY_ID`: AWS access key with write permissions on bucket
- `AWS_SECRET_ACCESS_KEY`: AWS access key secret

### CLI Arguments:

- `--start`: Date (in UTC) of earliest records to be fetched in format `YYYY-MM-DD`. Defaults to yesterday.
- `--end`: Date (in UTC) of the most recent records to be fetched in format `YYYY-MM-DD`. Defaults to today.
- `-e/--env`: The runtime environment. `dev` or `prod`. This value applies to the S3 Object key of the uploaded file.
- `-v/--verbose`: Sets the logger level to DEBUG

### Usage:

Fetch/upload UTC yesterday's transactions

```shell
$ python txn_history.py --verbose
```

Fetch/upload yesterday's payment report

```shell
$ python txn_history.py --verbose --report payments
```

Fetch/upload transactions for Oct 1, 2021

```shell
$ python txn_history.py --start 2021-10-01 --end 2021-10-01 --verbose
```

Fetch/upload all transactions from October 2021, inclusive of start and end dates

```shell
$ python txn_history.py --start 2021-10-01 --end 2021-10-31 --verbose
```

The `--env` controls the S3 file path and defaults to `dev`.

```shell
# upload files to <bucket-name>/meters/<env>/txn_history/<current-year>/<current-month>/<current-date>.csv
$ python txn_history.py --env prod
```

***

## smartfolio_s3.py

This script takes the CSVs extracted from DR-Direct `transaction_history` table (which are stored in an S3 bucket) and stores them locally into two postgres databases.

### Environment variables

- `AWS_ACCESS_ID`: AWS access key with write permissions on bucket
- `AWS_PASS`: AWS access key secret
- `BUCKET_NAME`: S3 bucket name where data is stored
- `POSTGREST_TOKEN`: Postgrest token secret

### CLI Arguments:

- `--year`: Year of S3 folder to select, defaults to current year.
- `--month`: Month of S3 folder to select. defaults to current month.

### Usage Examples:

Upserts the current month's data from S3 to the two postgres DBs.
```shell
$ python smartfolio_s3.py 
```

Upserts 2021's data for the current month.
```shell
$ python smartfolio_s3.py --year 2021
```

Upserts data for June 2021.
```shell
$ python smartfolio_s3.py --year 2021 --month 6
```

### Databases

`flowbird_transactions_raw` - A database just for parking meter data which is provided by vendor Flowbird (AKA Smartfolio).

`transactions` - A combined parking database that includes data from parking meters but also app purchases (Passport is the vendor).

***

## payments_s3.py

This script takes the CSVs extracted from DR-Direct credit card payment supervision (`archipel_transactionspub`) table (which are stored in an S3 bucket) and stores them in a postgres database. It is similar to the above `smartfolio_s3.py` but there are different data processing steps and is stored in a separate databse used for financial reporting.


### Environment variables

- `AWS_ACCESS_ID`: AWS access key with write permissions on bucket
- `AWS_PASS`: AWS access key secret
- `BUCKET_NAME`: S3 bucket name where data is stored
- `POSTGREST_TOKEN`: Postgrest token secret

### CLI Arguments:

- `--year`: Year of S3 folder to select, defaults to current year.
- `--month`: Month of S3 folder to select. defaults to current month.


### Usage Examples:

Upserts the current month's data from S3 to the two postgres DBs.
```shell
$ python payments_s3.py 
```

Upserts 2021's data for the current month.
```shell
$ python payments_s3.py --year 2021
```

Upserts data for June 2021.
```shell
$ python payments_s3.py --year 2021 --month 6
```

### Databases

`flowbird_payments_raw` - Where the credit card payment supervision table is stored in postgres.


***

# Fiserv Email publishing

Three daily emails are scheduled to arrive at a S3 email address with processed credit card payments data for Austin's parking transactions. 

fiserv_email_pub.py takes the emails which are stored in S3 and parses out the attachment CSVs and places them in a separate folder.

## S3 Folder layout:
```
-> emails (received emails arrive here)
	-> current_processed (processed csv files placed here)
	-> processed (legacy processed files were placed here)
	-> archive (old emails are moved to here)
   ```

## fiserv_DB.py

Take the processed email attachments stored in S3 and upsert them to a postgres DB.

### Environment variables

- `POSTGREST_TOKEN`: Token secret used by postgREST client
- `ENDPOINT`: Dr-Direct service endpoint
- `BUCKET_NAME`: S3 bucket name
- `AWS_ACCESS_ID`: AWS access key with write permissions on bucket
- `AWS_PASS`: AWS access key secret
- `FSRV_EMAIL`: The email address of the automated emails that are delivered by Fiserv 

### CLI Arguments:

Note that folders are organized by Fiserv automated email sent date but contains data up to 7 days prior.
- `--year`: Year of S3 folder to select, defaults to current year. 
- `--month`: Month of S3 folder to select, defaults to current year. 

### Usage Examples

Upserts the current month's data from S3 to the two postgres DBs.
```shell
$ python fiserv_DB.py 
```

Upserts 2021's data for the current month.
```shell
$ python fiserv_DB.py --year 2021
```

Upserts data for June 2021.
```shell
$ python fiserv_DB.py --year 2021 --month 6
```

### Database Tables

`fiserv_reports_raw` - Stores the Fiserv reports.

## match_field_processing.py
This script looks at the `fiserv_reports_raw` and `flowbird_payments_raw` databases and checks for matches in the field called `match_field`. It then updates the `fiserv_reports_raw` table with the unique ID of the flowbird payment (`flowbird_id`). 

### Environment variables

- `POSTGREST_TOKEN`: Token secret used by postgREST client

### CLI Arguments:

- `--start`: Date (in UTC) of earliest `flowbird` records to be searched for matches (YYYY-MM-DD). Defaults to 2022-01-01.
- `--end`: Date (in UTC) of the most recent `flowbird` records to be searched for matches (YYYY-MM-DD). Defaults to today.

Note: CLI arguments only select which rows of `flowbird_payments_raw` to search. It will search for matches on all `flowbird_id is NULL` records in `fiserv_reports_raw`

### Usage Examples

Update `flowbird_id` based on all flowbird records updated between January 1st, 2022 and today
```shell
$ python match_field_processing.py 
```

Update `flowbird_id` based on all flowbird records updated between March 11th, 2022 and today
```shell 
$ python match_field_processing.py --start 2022-03-11
```

Update `flowbird_id` based on all flowbird records updated between March 11th, 2022 and December 1st, 2022
```shell
$ python match_field_processing.py --start 2022-03-11 --end 2022-12-01
```

***

## parking_socrata.py
This script will publish any or all of the four different Socrata datasets for defined for parking tranasctions. They are stored locally in a postgres database.

### Postgres Tables

`transactions` - Combined passport and flowbird (AKA smartfolio) vendor parking data. This will represent the comprehensive Austin parking dataset.
`flowbird_transactions_raw` - Flowbird parking transactions
`flowbird_payments_raw` - Where the credit card payment supervision table is stored in postgres. AKA: `archipel_transactionspub`
`fiserv_reports_raw` - Merchant processor for Flowbird. These are should match the processed payments in `flowbird_payments_raw`.  

### Socrata Datasets
Also defined as envriomential variables.
(Socrata dataset) -> (postgrest table)

-  `TXNS_DATASET`     ->   `transactions`
-  `METERS_DATASET`   ->   `flowbird_transactions_raw`
-  `PAYMENTS_DATASET` ->   `flowbird_payments_raw`
-  `FISERV_DATASET`   ->   `fiserv_reports_raw`

### Environment variables

- `POSTGREST_TOKEN`: Postgrest token secret
- `SO_WEB`: URL of socrata that is being published to such as: `data.austintexas.gov`
- `SO_TOKEN`: App token secret for Socrata
- `SO_USER`: Username of Socrata admin account
- `SO_PASS`: Password of Socrata admin account

### CLI Arguments:
- `--dataset`: Dataset name to upload to Socrata (fiserv, meters, payments, transactions, all). Defaults to `all`.
- `--start`: Date (in UTC) of earliest records to be uploaded (YYYY-MM-DD) based on when records where last updated (`updated_at`). Defaults to yesterday.
- `--end`: Date (in UTC) of most recent records to be uploaded (YYYY-MM-DD) based on when records where last updated (`updated_at`). Defaults to today.


### Usage Examples:

Upsert data to Socrata for all four datasets for data that was recently updated in the last day (UTC).
```shell
$ python parking_socrata.py 
```

Upsert data to Socrata for only the parking meters dataset for data that was recently updated in the last day (UTC).
```shell
$ python parking_socrata.py --dataset meters
```

Upsert data to Socrata for only the transactions dataset for data that has been updated since January 1st 2021 (UTC).
```shell
$ python parking_socrata.py --dataset transactions -- start 2021-01-01
```

Upsert data to Socrata for only the transactions dataset for data that has been updated between January 1st 2021 and July 1st 2021 (UTC).
```shell
$ python parking_socrata.py --dataset transactions -- start 2021-01-01 --end 2021-07-01
```

***

### Docker

A Github action is configured to build/push this subdirectory to DTS docker hub with image name `atd-parking-data-meters`.

Here's a sample command to run the transaction history upload with an environment file.

```bash
docker run --rm -it --env-file env_file atddocker/atd-parking-data-meters python txn_history.py -v --start 2021-12-08 --end 2021-12-09
```