"""
Redeem all resolved positions — converts winning outcome tokens back to USDC.
Called by Discord bot via: from engine.redeem import main
"""

import os
import sys
import time
import httpx

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from eth_account import Account


def main():
    pk = os.environ.get("PRIVATE_KEY", "")
    rpc = os.environ.get("POLYGON_RPC_URL", "")

    if not pk or not rpc:
        print("ERROR: Missing PRIVATE_KEY or POLYGON_RPC_URL")
        return

    eoa = Account.from_key(pk).address
    w3 = Web3(Web3.HTTPProvider(rpc))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    print(f"EOA: {eoa}")

    # Check USDC balance before
    USDC_ADDR = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
    USDC_ABI = [{"constant": True, "inputs": [{"name": "owner", "type": "address"}],
                 "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
    usdc = w3.eth.contract(address=Web3.to_checksum_address(USDC_ADDR), abi=USDC_ABI)
    bal_before = usdc.functions.balanceOf(Web3.to_checksum_address(eoa)).call()
    print(f"USDC before: ${bal_before / 1e6:.2f}")

    # ConditionalTokens contract
    CT_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
    CT_ABI = [
        {"inputs": [
            {"internalType": "contract IERC20", "name": "collateralToken", "type": "address"},
            {"internalType": "bytes32", "name": "parentCollectionId", "type": "bytes32"},
            {"internalType": "bytes32", "name": "conditionId", "type": "bytes32"},
            {"internalType": "uint256[]", "name": "indexSets", "type": "uint256[]"}
        ], "name": "redeemPositions", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
    ]
    ct = w3.eth.contract(address=Web3.to_checksum_address(CT_ADDR), abi=CT_ABI)

    PARENT = bytes(32)
    INDEX_SETS = [1, 2]
    CHAIN_ID = 137

    # Fetch positions
    print("Fetching positions...")
    http = httpx.Client(timeout=15.0)

    try:
        resp = http.get(f"https://data-api.polymarket.com/positions?user={eoa}")
        positions = resp.json() if resp.status_code == 200 else []
        print(f"Found {len(positions)} positions")
    except Exception as e:
        print(f"Error fetching positions: {e}")
        positions = []

    redeemable = [p for p in positions if p.get("redeemable")]
    print(f"Redeemable: {len(redeemable)}")

    redeemed = 0
    for pos in redeemable:
        title = pos.get("title", "") or pos.get("slug", "")
        condition_id = pos.get("conditionId", "") or pos.get("condition_id", "")
        cur_value = float(pos.get("currentValue", 0) or 0)
        size = float(pos.get("size", 0) or 0)

        if not condition_id:
            # Try Gamma API fallback
            asset = pos.get("asset", "")
            if asset:
                try:
                    resp = http.get("https://gamma-api.polymarket.com/markets", params={"clob_token_ids": asset})
                    if resp.status_code == 200:
                        markets = resp.json()
                        if markets:
                            condition_id = markets[0].get("conditionId", "")
                except Exception:
                    pass

        if not condition_id:
            print(f"  SKIP (no conditionId): {title[:50]}")
            continue

        print(f"Redeeming: {title[:50]} ({size:.1f} shares, value=${cur_value:.2f})")

        try:
            cid_bytes = bytes.fromhex(condition_id.replace("0x", ""))
            nonce = w3.eth.get_transaction_count(Web3.to_checksum_address(eoa))

            tx = ct.functions.redeemPositions(
                Web3.to_checksum_address(USDC_ADDR),
                PARENT,
                cid_bytes,
                INDEX_SETS,
            ).build_transaction({
                "chainId": CHAIN_ID,
                "from": Web3.to_checksum_address(eoa),
                "nonce": nonce,
            })

            signed = w3.eth.account.sign_transaction(tx, private_key=pk)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            print(f"  Sent: {tx_hash.hex()}")

            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            status = "OK" if receipt["status"] == 1 else "FAILED"
            print(f"  [{status}] gas: {receipt['gasUsed']}")
            if receipt["status"] == 1:
                redeemed += 1

            time.sleep(2)  # Avoid nonce issues

        except Exception as e:
            err = str(e)
            if "execution reverted" in err.lower():
                print(f"  SKIP (already redeemed or no tokens)")
            else:
                print(f"  ERROR: {err[:100]}")

    http.close()

    # Check USDC balance after
    bal_after = usdc.functions.balanceOf(Web3.to_checksum_address(eoa)).call()
    gained = (bal_after - bal_before) / 1e6
    print(f"USDC after:  ${bal_after / 1e6:.2f}")
    print(f"Gained:      ${gained:.2f}")
    print(f"Redeemed:    {redeemed} positions")


if __name__ == "__main__":
    main()
