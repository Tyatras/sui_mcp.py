import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time
import json

DEBUG = True  # 🔁 Toggle to False to silence logs

# Google Sheets setup
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")
wallet_address = config_ws.acell("B1").value.strip()

existing_hashes = set(output_ws.col_values(3)[1:])  # Skip header

rpc_url = "https://fullnode.mainnet.sui.io"
cursor = None
page_size = 50
has_next_page = True
rows_to_append = []

print(f"🔄 Starting sync for: {wallet_address}")

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
    cursor = result.get("nextCursor")
    has_next_page = result.get("hasNextPage", False)

    if DEBUG:
        print(f"🧭 Got {len(txns)} txns | nextCursor: {cursor} | hasNext: {has_next_page}")

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue

        # Timestamp
        timestamp = txn.get("timestampMs")
        ts_fmt = datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""

        # === FEE ===
        fee = "0"
        try:
            total_gas_used = txn.get("effects", {}).get("gasUsed", {}).get("totalGasUsed", 0)
            fee = f"{float(total_gas_used) / 1e9:.9f}"
        except Exception as e:
            if DEBUG:
                print(f"⚠️ Fee parse fail [{digest}]: {e}")

        # === AMOUNT & DIRECTION ===
        amount = ""
        direction = ""

        try:
            events = txn.get("events", [])
            if DEBUG:
                print(f"📦 {digest} has {len(events)} events")

            for event in events:
                if "moveEvent" in event:
                    move = event["moveEvent"]
                    fields = move.get("fields", {})

                    amt_raw = fields.get("amount")
                    sender = fields.get("sender")
                    recipient = fields.get("recipient")

                    if amt_raw:
                        amount = f"{int(amt_raw) / 1e9:.9f}"
                    if sender and recipient:
                        direction = "OUT" if sender == wallet_address else "IN"
                    break

        except Exception as e:
            if DEBUG:
                print(f"❌ Event parse fail [{digest}]: {e}")
                print(json.dumps(txn.get("events", []), indent=2))

        row = [
            ts_fmt,
            wallet_address,
            digest,
            "SUI",
            amount,
            fee,
            direction
        ]
        rows_to_append.append(row)

    time.sleep(0.5)

# Upload
if rows_to_append:
    print(f"⬆️ Appending {len(rows_to_append)} rows...")
    for row in reversed(rows_to_append):
        output_ws.append_row(row)
else:
    print("✅ No new rows found")

print("🎯 Sync complete.")
