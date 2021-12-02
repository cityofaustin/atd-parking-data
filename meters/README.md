
Scripts for processing parking meter transactions from the Flowbird DR-Direct tool.

## txn_history.py

Fetch Flowbird meter transactions and load to S3. This processes the "txn_history" report from the Flowbird Dr-Direct tool.

One file is generated per day in the provided range and uploaded to S3 at: `<bucket-name>/meters/transaction_history/year/month/<query-start-date>.csv`

### Environmental variables
- `USER`: Dr-direct username
- `PASSWORD`: Dr-Direct password
- `ENDPOINT`: Dr-Direct service endpoint
- `BUCKET`: S3 bucket name


### CLI Arguments:
-  `--start`: Date (in UTC) of earliest records to be fetched in format `YYYY-MM-DD`. Defaults to yesterday.
- `--end`: Date (in UTC) of the most recent records to be fetched in format `YYYY-MM-DD`. Defaults to yesterday.
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
