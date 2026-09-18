"""One-off helper: create a Lighter API key and print the three values the delta bot needs.

Lighter has no web UI for programmatic API keys — they're created via the SDK, authorised on-chain by
your Lighter wallet's key. Run this ONCE, locally, then paste the printed values into the bot's
🔑 Keys → Lighter. Your L1 key is used only to sign the on-chain registration here and is never
stored by the bot afterward (the bot uses only the generated API key).

Usage (PowerShell):
  $env:LIGHTER_ETH_KEY="0x<your Lighter wallet private key>"
  # optional: choose a slot 4..254 (0-3 are reserved for the Lighter app); default 4
  $env:LIGHTER_API_KEY_INDEX="4"
  python scripts/create_lighter_key.py

Needs the SDK:  pip install git+https://github.com/elliottech/lighter-python.git
"""

from __future__ import annotations

import asyncio
import os

import lighter
from eth_account import Account as EthAccount

API_URL = os.getenv("LIGHTER_API_URL", "https://api.rh.lighter.xyz").strip()
ETH_KEY = os.getenv("LIGHTER_ETH_KEY", "").strip()
API_KEY_INDEX = int(os.getenv("LIGHTER_API_KEY_INDEX", "4"))


async def main() -> None:
    if not ETH_KEY:
        raise SystemExit("set LIGHTER_ETH_KEY to your Lighter wallet's private key first")
    if not (4 <= API_KEY_INDEX <= 254):
        raise SystemExit("LIGHTER_API_KEY_INDEX must be 4..254 (0-3 are reserved for the app)")

    eth_address = EthAccount.from_key(ETH_KEY).address
    print(f"wallet: {eth_address}")

    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=API_URL))
    try:
        resp = await lighter.AccountApi(api_client).accounts_by_l1_address(l1_address=eth_address)
        account_index = resp.sub_accounts[0].index
        print(f"account index: {account_index}")

        # Generate the new API key pair and register its public half on-chain, signed by the L1 key.
        private_key, public_key, err = lighter.create_api_key()
        if err:
            raise SystemExit(f"create_api_key failed: {err}")

        signer = lighter.SignerClient(
            url=API_URL,
            account_index=account_index,
            api_private_keys={API_KEY_INDEX: private_key},
            chain_id=None,  # auto-detected from the URL
        )
        try:
            _resp, err = await signer.change_api_key(
                eth_private_key=ETH_KEY, new_pubkey=public_key, api_key_index=API_KEY_INDEX,
            )
            if err:
                raise SystemExit(f"change_api_key failed: {err}")
            # Give the server a moment, then confirm the key is live.
            await asyncio.sleep(2)
            if (err := signer.check_client()):
                print(f"note: check_client says: {err} (it can lag a few seconds after registration)")
        finally:
            await signer.close()
    finally:
        await api_client.close()

    print("\n=== paste these into the bot: 🔑 Ключі → Lighter ===")
    print(f"API key's private key : {private_key}")
    print(f"Account Index         : {account_index}")
    print(f"API Key Index         : {API_KEY_INDEX}")


if __name__ == "__main__":
    asyncio.run(main())
