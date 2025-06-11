import gspread
from google.oauth2.service_account import Credentials
import requests
from datetime import datetime
import time

DEBUG = True  # Set to False in production

# Setup Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
client = gspread.authorize(creds)

spreadsheet = client.open("SUI_Transactions")
config_ws = spreadsheet.worksheet("Config")
output_ws = spreadsheet.worksheet("Transactions")
wallet_address = config_ws.acell("B1").value.strip().lower()

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
                    "All": []
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
        print(f"🧭 {len(txns)} transactions fetched | Next page: {has_next_page}")

    for txn in txns:
        digest = txn.get("digest")
        if digest in existing_hashes:
            continue

        timestamp = txn.get("timestampMs")
        ts_fmt = datetime.utcfromtimestamp(int(timestamp) / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""

        # === FEE ===
        fee = "0"
        try:
            gas_used = txn.get("effects", {}).get("gasUsed", {}).get("totalGasUsed", 0)
            fee = f"{int(gas_used) / 1e9:.9f}"
        except Exception as e:
            if DEBUG:
                print(f"⚠️ Fee parse failed [{digest}]: {e}")

        # === BALANCE CHANGES ===
        changes = txn.get("balanceChanges", [])
        for change in changes:
            owner = change.get("owner", "")
            coin_type = change.get("coinType", "")
            amount_raw = change.get("amount")

            # Handle owner field format (dict or string)
            if isinstance(owner, dict):
                owner_str = list(owner.values())[0]
            else:
                owner_str = owner

            # DEBUG: Show balance change evaluation
            if DEBUG:
                print(f"🧩 digest={digest} | owner={owner_str} | wallet={wallet_address} | match={wallet_address in owner_str.lower()} | amt={amount_raw}")

            # Match based on lowercase address presence
            if not (owner_str and wallet_address in owner_str.lower() and amount_raw):
                continue

            token_symbol = "SUI" if coin_type.endswith("::sui::SUI") else coin_type.split("::")[-1]
            direction = "IN" if int(amount_raw) > 0 else "OUT"
            amount = f"{abs(int(amount_raw)) / 1e9:.9f}"

            row = [
                ts_fmt,
                wallet_address,
                digest,
                token_symbol,
                amount,
                fee,
                direction
            ]
            rows_to_append.append(row)

    time.sleep(0.5)

# === WRITE TO SHEET ===
if rows_to_append:
    print(f"⬆️ Appending {len(rows_to_append)} new rows...")
    for row in reversed(rows_to_append):
        output_ws.append_row(row)
else:
    print("✅ No new transactions found.")

print("🎯 Sync complete.")
