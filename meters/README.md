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
