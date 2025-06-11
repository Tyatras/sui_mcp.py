import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time
import json

DEBUG = True  # Set to False in production

# Setup Google Sheets
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

print(f"🔄 Syncing for wallet: {wallet_address}")

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
        print(f"🧭 {len(txns)} transactions fetched | Next: {has_next_page}")

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue

        timestamp = txn.get("timestampMs")
        ts_fmt = datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""

        # === FEE ===
        fee = "0"
        try:
            gas = txn.get("effects", {}).get("gasUsed", {})
            total_mist = int(gas.get("computationCost", 0)) + int(gas.get("storageCost", 0)) - int(gas.get("storageRebate", 0))
            fee = f"{total_mist / 1e9:.9f}"
        except Exception as e:
            if DEBUG:
                print(f"⚠️ Gas parse fail [{digest}]: {e}")

        # === AMOUNT & DIRECTION ===
        amount = ""
        direction = ""
        matched = False
        try:
            for event in txn.get("events", []):
                if "moveEvent" not in event:
                    continue

                move = event["moveEvent"]
                evt_type = move.get("type", "")
                fields = move.get("fields", {})

                # Match based on known token transfer types
                if "Pay" in evt_type or "TransferObject" in evt_type:
                    amt_raw = fields.get("amount")
                    sender = fields.get("sender")
                    recipient = fields.get("recipient")

                    if amt_raw:
                        amount = f"{int(amt_raw) / 1e9:.9f}"
                    if sender and recipient:
                        direction = "OUT" if sender == wallet_address else "IN"
                    matched = True
                    break

            if not matched:
                if DEBUG:
                    print(f"🔍 No matching moveEvent found for {digest}, checking balanceChanges")
                for change in txn.get("balanceChanges", []):
                    owner = change.get("owner", "")
                    amt = change.get("amount")
                    coinType = change.get("coinType", "")

                    if wallet_address in owner and amt:
                        direction = "IN" if int(amt) > 0 else "OUT"
                        amount = f"{abs(int(amt)) / 1e9:.9f}"
                        matched = True
                        break

        except Exception as e:
            if DEBUG:
                print(f"❌ Event/balance parse fail [{digest}]: {e}")
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

# Append to sheet
if rows_to_append:
    print(f"⬆️ Appending {len(rows_to_append)} new rows...")
    for row in reversed(rows_to_append):
        output_ws.append_row(row)
else:
    print("✅ No new transactions found.")

print("🎯 Sync complete.")
