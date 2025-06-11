import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime

# Google Auth
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open spreadsheet and worksheets
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")

# Get wallet address from cell B1 in Config tab
wallet_address = config_ws.acell("B1").value

# Query SUI blockchain
response = requests.post("https://fullnode.mainnet.sui.io", json={
    "jsonrpc": "2.0",
    "method": "sui_getTransactions",
    "params": ["InputObject", wallet_address],
    "id": 1
})

txns = response.json().get("result", [])

# Write transactions to the Transactions tab
for txn in txns[:5]:  # limit to 5 for demo
    row = [
        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        wallet_address,
        txn.get("digest", "n/a"),
        "SUI", "", "", ""
    ]
    output_ws.append_row(row)

print(f"Synced {len(txns)} transactions for wallet {wallet_address}")
