import gspread
from google.oauth2.service_account import Credentials
import requests

# Setup Google Sheets credentials
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open the sheet
sheet = client.open("SUI_Transactions").sheet1

# Example wallet address (replace later with data from the sheet)
wallet_address = "0xabc123..."

# Query the SUI blockchain
response = requests.post("https://fullnode.mainnet.sui.io", json={
    "jsonrpc": "2.0",
    "method": "sui_getTransactions",
    "params": ["InputObject", wallet_address],
    "id": 1
})

txns = response.json().get("result", [])

# Append results to the sheet
for txn in txns[:5]:
    sheet.append_row(["", wallet_address, txn.get("digest", "n/a"), "SUI", "", "", ""])

print(f"Synced {len(txns)} transactions")
