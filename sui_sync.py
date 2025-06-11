import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime

# Authorize Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open the spreadsheet and worksheets
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")

# Get wallet address from Config tab (cell B1)
wallet_address = config_ws.acell("B1").value
print("Using wallet:", wallet_address)

# Query SUI blockchain for transactions from this address
rpc_url = "https://fullnode.mainnet.sui.io"
payload = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "suix_queryTransactionBlocks",
    "params": [
        {
            "filter": {
                "FromAddress": wallet_address
            },
            "options": {
                "showInput": True,
                "showEffects": True,
                "showEvents": True,
                "showBalanceChanges": True,
                "showObjectChanges": True
            }
        },
        None,     # cursor
        10,       # limit
        True      # descending order
    ]
}

response = requests.post(rpc_url, json=payload)
resp_json = response.json()
txns = resp_json.get("result", {}).get("data", [])

print(f"Found {len(txns)} transactions")

# Append data to Transactions tab
for txn in txns:
    row = [
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        wallet_address,
        txn.get("digest", "N/A"),
        "SUI",   # Placeholder for Token
        "",      # Amount
        "",      # Fee
        ""       # Direction
    ]
    output_ws.append_row(row)

print("Sync complete ✅")
