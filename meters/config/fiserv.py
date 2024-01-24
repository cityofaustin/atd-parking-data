REQUIRED_FIELDS = [
    "Invoice Number",
    "Txn Date",
    "Transaction Type",
    "Terminal ID",
    "Batch No.",
    "Batch Sequence ID",
    "Batch Date",
    "Funded Date",
    "Processed Sales Amount",
    "Transaction Status",
    "Site ID (BE)",
]

FIELD_MAPPING = {
    "Invoice Number": "invoice_id",
    "Txn Date": "transaction_date",
    "Transaction Type": "transaction_type",
    "Terminal ID": "meter_id",
    "Batch No.": "batch_number",
    "Batch Sequence ID": "batch_sequence_number",
    "Funded Date": "funded_date",
    "Processed Sales Amount": "amount",
    "Transaction Status": "transaction_status",
    "Site ID (BE)": "account",
    "Product Code": "card_type",
}
