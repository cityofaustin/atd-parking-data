Scripts for processing parking meter transactions from the Flowbird DR-Direct tool.

## txn_history.py

Fetch Flowbird meter transactions and load to S3. This processes the "txn_history" report from the Flowbird Dr-Direct tool.

One file is generated per day in the provided range and uploaded to S3 at: `<bucket-name>/meters/<environment>/transaction_history/year/month/<query-start-date>.csv`

### Environmental variables

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

### Docker

A Github action is configured to build/push this subdirectory to DTS docker hub with image name `atd-parking-data-meters`.

Here's a sample command to run the transaction history upload with an environment file.

```bash
docker run --rm -it --env-file env_file atddocker/atd-parking-data-meters python txn_history.py -v --start 2021-12-08 --end 2021-12-09
```


## smartfolio_s3.py

This script takes the CSVs extracted from DR-Direct `transaction_history` table (which are stored in an S3 bucket) and stores them locally into two postgres databases.

### Environmental variables

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

## payments_s3.py

This script takes the CSVs extracted from DR-Direct credit card payment supervision (`archipel_transactionspub`) table (which are stored in an S3 bucket) and stores them in a postgres database. It is similar to the above `smartfolio_s3.py` but there are different data processing steps and is stored in a separate databse used for financial reporting.


### Environmental variables

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
