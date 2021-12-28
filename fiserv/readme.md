# Fiserv Email publishing

Three daily emails are scheduled to arrive at a S3 email address with processed credit card payments data for Austin's parking transactions. 

fiserv_email_pub.py takes the emails which are stored in S3 and parses out the attachment CSVs and places them in a separate folder.

## S3 Folder layout:
```
-> emails (received emails arrive here)
	-> processed (processed csv files placed here)
	-> archive (old emails are moved to here)
   ```
