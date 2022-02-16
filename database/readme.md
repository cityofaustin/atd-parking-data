## parking_socrata.py
This script will publish any or all of the four different Socrata datasets for defined for parking tranasctions. They are stored locally in a postgres database.

### Postgres Tables involved

`transactions` - Combined passport and flowbird (AKA smartfolio) vendor parking data. This will represent the comprehensive Austin parking dataset.
`flowbird_transactions_raw` - Flowbird parking transactions
`flowbird_payments_raw` - Where the credit card payment supervision table is stored in postgres. AKA: `archipel_transactionspub`
`fiserv_reports_raw` - Merchant processor for Flowbird. These are should match the processed payments in `flowbird_payments_raw`.  

### Socrata Datasets involved
Also defined as envriomential variables.
(Socrata dataset) -> (postgrest table)

-  `TXNS_DATASET`     ->   `transactions`
-  `METERS_DATASET`   ->   `flowbird_transactions_raw`
-  `PAYMENTS_DATASET` ->   `flowbird_payments_raw`
-  `FISERV_DATASET`   ->   `fiserv_reports_raw`

### Environmental variables

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

