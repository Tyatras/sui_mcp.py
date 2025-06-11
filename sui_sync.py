import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time

# Authenticate with Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

# Access workbook
spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")
wallet_address = config_ws.acell("B1").value.strip()

print(f"📡 Syncing transactions for wallet: {wallet_address}")

# Avoid duplicates by reading existing Txn Hash column
existing_hashes = set(output_ws.col_values(3)[1:])  # Skip header row

# RPC config
rpc_url = "https://fullnode.mainnet.sui.io"
cursor = None
page_size = 50
has_next_page = True
new_rows = []

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

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue  # Already processed

        timestamp = txn.get("timestampMs")
        ts_fmt = datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""

        # Fee
        fee = "0"
        try:
            total_gas_used = txn.get("effects", {}).get("gasUsed", {}).get("totalGasUsed", 0)
            fee = f"{float(total_gas_used) / 1e9:.9f}"
        except Exception as e:
            print(f"⚠️ Fee parse error for {digest}: {e}")

        # Defaults
        amount = ""
        direction = ""

        # Events
        events = txn.get("events", [])
        for event in events:
            if "Pay" in event:
                pay = event["Pay"]
                try:
                    amounts = pay.get("amounts", [])
                    total_amount = sum([int(a) for a in amounts]) / 1e9
                    amount = f"{total_amount:.9f}"
                    direction = "OUT"
                except:
                    pass
                break
            elif "TransferObject" in event:
                details = event["TransferObject"]
                sender = details.get("sender", "")
                recipient = details.get("recipient", "")
                direction = "OUT" if sender == wallet_address else "IN"
                break

        print(f"✔️ {ts_fmt} | {digest} | Amount: {amount} | Fee: {fee} | Direction: {direction}")

        row = [
            ts_fmt,
            wallet_address,
            digest,
            "SUI",
            amount,
            fee,
            direction
        ]
        new_rows.append(row)

    time.sleep(0.5)  # Prevent rate limits

# Append rows
if new_rows:
    print(f"🚀 Appending {len(new_rows)} new transactions...")
    for row in reversed(new_rows):
        output_ws.append_row(row)
else:
    print("✅ No new transactions to append.")

print("🎯 Sync complete.")
