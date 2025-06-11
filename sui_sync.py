import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time

# ========== SETUP ==========
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")
wallet_address = config_ws.acell("B1").value.strip()

existing_hashes = set(output_ws.col_values(3)[1:])  # Column C = Txn Hash

# ========== FETCH LOGIC ==========
rpc_url = "https://fullnode.mainnet.sui.io"
cursor = None
page_size = 50
has_next_page = True
rows_to_append = []

print(f"🔍 Syncing for wallet: {wallet_address}")

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
    cursor = data.get("nextCursor")
    has_next_page = data.get("hasNextPage", False)

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue

        timestamp = txn.get("timestampMs")
        ts_fmt = datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""

        # === FEE ===
        try:
            total_gas_used = txn.get("effects", {}).get("gasUsed", {}).get("totalGasUsed", 0)
            fee = f"{float(total_gas_used) / 1e9:.9f}"
        except:
            fee = ""

        # === AMOUNT & DIRECTION ===
        amount = ""
        direction = ""
        try:
            for event in txn.get("events", []):
                if "moveEvent" in event:
                    fields = event["moveEvent"].get("fields", {})
                    if "amount" in fields:
                        raw_amount = int(fields["amount"])
                        amount = f"{raw_amount / 1e9:.9f}"
                    if "sender" in fields and "recipient" in fields:
                        sender = fields["sender"]
                        recipient = fields["recipient"]
                        direction = "OUT" if sender == wallet_address else "IN"
                    break
        except Exception as e:
            print(f"⚠️ Event parse error on {digest}: {e}")

        rows_to_append.append([
            ts_fmt,
            wallet_address,
            digest,
            "SUI",
            amount,
            fee,
            direction
        ])

    time.sleep(0.5)  # rate-limiting

# ========== INSERT TO SHEET ==========
if rows_to_append:
    print(f"🚀 Appending {len(rows_to_append)} transactions to sheet...")
    output_ws.insert_rows(rows_to_append[::-1], row=2)  # insert after header
else:
    print("✅ No new transactions found.")

print("🎯 Sync complete.")
