REQUIRED_FIELDS = [
    "Invoice Number",
    "Account #",
    "Txn Date",
    "Transaction Type",
    "Terminal ID",
    "Batch No.",
    "Batch Sequence ID",
    "Funded Date",
    "Record Date",
    "Processed Sales Amount",
    "Transaction Status",
    "Site ID (BE)",
]

FIELD_MAPPING = {
    "Invoice Number": "invoice_id",
    "Account #": "match_field",
    "Txn Date": "transaction_date",
    "Transaction Type": "transaction_type",
    "Terminal ID": "meter_id",
    "Batch No.": "batch_number",
    "Batch Sequence ID": "batch_sequence_number",
    "Batch Date": "submit_date",
    "Record Date": "funded_date",
    "Processed Sales Amount": "amount",
    "Transaction Status": "transaction_status",
    "Site ID (BE)": "account",
}
