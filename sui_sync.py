import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time

# Google Sheets setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Open your Google Sheet and get wallet from Config tab
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")
wallet_address = config_ws.acell("B1").value.strip()

# Load all existing hashes to avoid duplicates
existing_hashes = set(output_ws.col_values(3)[1:])  # Skip header

# SUI RPC setup
rpc_url = "https://fullnode.mainnet.sui.io"
cursor = None
page_size = 50
has_next_page = True
new_rows = []

print(f"Syncing transactions for: {wallet_address}")

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
    result = response.json().get("result", {})
    txns = result.get("data", [])
    cursor = result.get("nextCursor", None)
    has_next_page = result.get("hasNextPage", False)

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue

        # Timestamp (convert from ms to UTC)
        timestamp_ms = txn.get("timestampMs")
        timestamp = datetime.utcfromtimestamp(int(timestamp_ms)/1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp_ms else ""

        # Fee (convert from Mist to SUI)
        gas_info = txn.get("effects", {}).get("gasUsed", {})
        fee_sui = float(gas_info.get("totalGasUsed", 0)) / 1e9

        # Default placeholders
        direction = ""
        amount_sui = 0

        # Event inspection
        events = txn.get("events", [])
        for event in events:
            if "TransferObject" in event:
                obj = event["TransferObject"]
                sender = obj.get("sender", "")
                recipient = obj.get("recipient", "")
                direction = "OUT" if sender == wallet_address else "IN"
                break
            elif "Pay" in event:
                pay = event["Pay"]
                recipients = pay.get("recipients", [])
                amounts = pay.get("amounts", [])
                total_mist = sum([int(a) for a in amounts])
                amount_sui = total_mist / 1e9
                direction = "OUT"
                break

        # Only convert amount if it's been found
        amount_str = f"{amount_sui:.9f}" if amount_sui > 0 else ""

        row = [
            timestamp,
            wallet_address,
            digest,
            "SUI",
            amount_str,
            f"{fee_sui:.9f}",
            direction
        ]
        new_rows.append(row)

    time.sleep(0.5)  # prevent rate limits

print(f"Appending {len(new_rows)} new transactions...")

# Append in reverse chronological order
for row in reversed(new_rows):
    output_ws.append_row(row)

print("✅ Sync complete.")
