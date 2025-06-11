import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime

# Google Sheets auth
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open spreadsheet and worksheets
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")

# Read wallet address from Config tab (cell B1)
wallet_address = config_ws.acell("B1").value
print("Using wallet:", wallet_address)

# SUI RPC query using correct method
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
        None,
        10,
        True
    ]
}

response = requests.post(rpc_url, json=payload)
resp_json = response.json()
txns = resp_json.get("result", {}).get("data", [])

print(f"Found {len(txns)} transactions")

# Write each transaction to the Transactions tab
for txn in txns:
    digest = txn.get("digest", "N/A")

    # Timestamp conversion
    timestamp_ms = txn.get("timestampMs")
    txn_time = datetime.utcfromtimestamp(int(timestamp_ms)/1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp_ms else "N/A"

    # Fee (from gasUsed)
    fee = ""
    try:
        fee = str(txn.get("effects", {}).get("gasUsed", {}).get("computationCost", ""))
    except Exception:
        fee = ""

    # Direction (based on event detection)
    direction = ""
    events = txn.get("events", [])
    for event in events:
        if "TransferObject" in event:
            direction = "OUT"
            break

    # Placeholder for Amount (can be parsed later)
    amount = ""

    row = [
        txn_time,
        wallet_address,
        digest,
        "SUI",
        amount,
        fee,
        direction
    ]

    output_ws.append_row(row)

print("✅ Sync complete.")
