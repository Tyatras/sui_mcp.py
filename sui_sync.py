import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time

# Google Sheets authentication
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open the spreadsheet and sheets
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")

# Get wallet address from Config sheet
wallet_address = config_ws.acell("B1").value
print("Wallet address:", wallet_address)

# Fetch all existing txn hashes in sheet to avoid duplicates
existing_hashes = set()
existing_data = output_ws.col_values(3)  # Column C: Txn Hash
for digest in existing_data[1:]:  # Skip header
    existing_hashes.add(digest)

# SUI RPC setup
rpc_url = "https://fullnode.mainnet.sui.io"
cursor = None
all_new_txns = []
page_size = 50
has_next_page = True

print("Fetching transaction history...")

while has_next_page:
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
            cursor,
            page_size,
            True
        ]
    }

    response = requests.post(rpc_url, json=payload)
    data = response.json().get("result", {})
    txns = data.get("data", [])
    cursor = data.get("nextCursor", None)
    has_next_page = data.get("hasNextPage", False)

    print(f"Retrieved {len(txns)} txns (nextCursor: {cursor})")

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue  # Skip if already in sheet

        # Timestamp
        timestamp_ms = txn.get("timestampMs")
        txn_time = datetime.utcfromtimestamp(int(timestamp_ms)/1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp_ms else "N/A"

        # Fee (convert from Mist to SUI)
        fee_mist = txn.get("effects", {}).get("gasUsed", {}).get("computationCost", 0)
        fee_sui = str(int(fee_mist) / 1e9)

        # Default values
        amount = ""
        direction = ""

        # Detect amount + direction from events
        events = txn.get("events", [])
        for event in events:
            if "TransferObject" in event:
                details = event["TransferObject"]
                recipient = details.get("recipient", "")
                sender = details.get("sender", "")
                direction = "OUT" if sender == wallet_address else "IN"
                break
            elif "Pay" in event:
                details = event["Pay"]
                recipient_list = details.get("recipients", [])
                amounts = details.get("amounts", [])
                total = sum([int(a) for a in amounts]) / 1e9
                amount = str(total)
                direction = "OUT" if wallet_address in details.get("inputCoins", []) else "IN"
                break

        # Row format
        row = [
            txn_time,
            wallet_address,
            digest,
            "SUI",
            amount,
            fee_sui,
            direction
        ]
        all_new_txns.append(row)

    # Optional: throttle requests to avoid rate limiting
    time.sleep(0.5)

# Append new transactions
if all_new_txns:
    print(f"Appending {len(all_new_txns)} new transactions...")
    for row in reversed(all_new_txns):  # Oldest first
        output_ws.append_row(row)
else:
    print("No new transactions to append.")

print("✅ Full sync complete.")
