# Fiserv Email publishing

Three daily emails are scheduled to arrive at a S3 email address with processed credit card payments data for Austin's parking transactions. 

fiserv_email_pub.py takes the emails which are stored in S3 and parses out the attachment CSVs and places them in a separate folder.

## S3 Folder layout:
```
-> emails (received emails arrive here)
	-> processed (processed csv files placed here)
	-> archive (old emails are moved to here)
   ```

## fiserv_DB.py

Take the processed email attachments stored in S3 and upsert them to a postgres DB.

### Environmental variables

- `POSTGREST_TOKEN`: Token secret used by postgREST client
- `ENDPOINT`: Dr-Direct service endpoint
- `BUCKET_NAME`: S3 bucket name
- `AWS_ACCESS_ID`: AWS access key with write permissions on bucket
- `AWS_PASS`: AWS access key secret

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

### Environmental variables

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
