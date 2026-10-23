"""Fintech: accounts (CSV) + transactions (JSONL)."""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta, timezone

from pipeline.generators.base import DomainGenerator, GeneratedBatch, Scenario, money, random_ts, utc_ts

COUNTRY_CURRENCY = {"US": "USD", "MX": "MXN", "CO": "COP", "ES": "EUR", "BR": "BRL"}
FIRST_NAMES = ["Ana", "Luis", "Maria", "Carlos", "Lucia", "John", "Emma", "Pedro", "Sofia", "Diego"]
LAST_NAMES = ["Garcia", "Smith", "Lopez", "Silva", "Perez", "Brown", "Gomez", "Rossi", "Diaz", "Moreno"]
CREDIT_TYPES = ("deposit", "transfer_in")
DEBIT_TYPES = ("withdrawal", "transfer_out", "card_payment", "fee")
MCC_CODES = ("5411", "5812", "5999", "4111", "5732")


def signed(txn: dict) -> float:
    return float(txn["amount"]) if txn["txn_type"] in CREDIT_TYPES else -float(txn["amount"])


class FintechGenerator(DomainGenerator):
    domain = "fintech"
    generic_targets = {
        "missing_column": ("transactions", "amount"),
        "missing_file": "accounts",
        "malformed_record": "transactions",
        "empty_file": "transactions",
    }

    def generate_clean(self, day: date, rows: int, rng: random.Random):
        accounts = []
        for i in range(1, max(5, rows // 10) + 1):
            country = rng.choice(list(COUNTRY_CURRENCY))
            accounts.append({
                "account_id": f"ACC-{i:05d}",
                "customer_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "country": country,
                "currency": COUNTRY_CURRENCY[country],
                "opened_at": day - timedelta(days=rng.randint(30, 2000)),
                "status": rng.choices(["active", "frozen", "closed"], [85, 10, 5])[0],
                "credit_limit": money(rng.choice([0, 500, 1000, 5000])),
            })
        accounts[0]["status"] = "active"
        active = [a for a in accounts if a["status"] == "active"]

        transactions = []
        for n in range(1, rows + 1):
            account = rng.choice(active)
            is_credit = rng.random() < 0.4
            txn_type = rng.choice(CREDIT_TYPES if is_credit else DEBIT_TYPES)
            amount = rng.uniform(100, 3000) if is_credit else rng.uniform(1, 400)
            transactions.append({
                "transaction_id": f"TXN-{day:%Y%m%d}-{n:06d}",
                "account_id": account["account_id"],
                "txn_ts": utc_ts(random_ts(rng, day)),
                "txn_type": txn_type,
                "amount": money(amount),
                "currency": account["currency"],
                "merchant_category": rng.choice(MCC_CODES) if txn_type == "card_payment" else None,
                "status": rng.choices(["posted", "pending", "reversed"], [90, 8, 2])[0],
            })

        # Business invariant: no account ends the day beyond its credit limit.
        limits = {a["account_id"]: float(a["credit_limit"]) for a in accounts}
        net: dict[str, float] = {}
        for t in transactions:
            if t["status"] == "posted":
                net[t["account_id"]] = net.get(t["account_id"], 0.0) + signed(t)
        for account_id, flow in net.items():
            if flow < -limits[account_id]:
                transactions.append({
                    "transaction_id": f"TXN-{day:%Y%m%d}-{len(transactions) + 1:06d}",
                    "account_id": account_id,
                    "txn_ts": utc_ts(random_ts(rng, day)),
                    "txn_type": "deposit",
                    "amount": money(-flow - limits[account_id] + 50),
                    "currency": next(a["currency"] for a in accounts if a["account_id"] == account_id),
                    "merchant_category": None,
                    "status": "posted",
                })
        return {"accounts": accounts, "transactions": transactions}

    def domain_scenarios(self) -> list[Scenario]:
        def sample(b: GeneratedBatch, rng, k=3, where=lambda t: True):
            return rng.sample([t for t in b.rows["transactions"] if where(t)], k)

        def duplicate_ids(b, rng):
            for t in sample(b, rng):
                b.rows["transactions"].append({**t, "amount": money(float(t["amount"]) + 1)})

        def malformed_amounts(b, rng):
            for t, bad in zip(sample(b, rng), ["12,50", "N/A", "1.2.3"]):
                t["amount"] = bad

        def null_ids(b, rng):
            for t in sample(b, rng, 2):
                t["transaction_id"] = None

        def negative_amounts(b, rng):
            for t in sample(b, rng):
                t["amount"] = -t["amount"]

        def above_limit(b, rng):
            sample(b, rng, 1, lambda t: t["txn_type"] in CREDIT_TYPES)[0]["amount"] = money(25000)

        def invalid_currency(b, rng):
            for t in sample(b, rng):
                t["currency"] = "XYZ"

        def currency_mismatch(b, rng):
            for t in sample(b, rng, 2):
                t["currency"] = "EUR" if t["currency"] != "EUR" else "USD"

        def orphan_accounts(b, rng):
            for t in sample(b, rng):
                t["account_id"] = "ACC-99999"

        def future_ts(b, rng):
            for t in sample(b, rng, 2):
                # Relative to now, not to the batch day: old batches + 30 days are still the past.
                t["txn_ts"] = utc_ts(datetime.now(timezone.utc) + timedelta(days=30))

        def card_without_merchant(b, rng):
            for t in sample(b, rng, 2, lambda t: t["txn_type"] == "card_payment"):
                t["merchant_category"] = None

        def closed_account_activity(b, rng):
            closed = next((a for a in b.rows["accounts"] if a["status"] == "closed"), None) or b.rows["accounts"][-1]
            closed["status"] = "closed"
            t = sample(b, rng, 1, lambda t: t["status"] == "posted")[0]
            t["account_id"], t["currency"] = closed["account_id"], closed["currency"]

        def overdraft(b, rng):
            account = b.rows["accounts"][0]
            account["credit_limit"] = money(0)
            # Every withdrawal stays under the single-transaction limit; only the daily total is wrong.
            remaining = 500 + sum(signed(t) for t in b.rows["transactions"]
                                  if t["account_id"] == account["account_id"] and t["status"] == "posted")
            n = 0
            while remaining > 0:
                n += 1
                amount = min(max(remaining, 1), 4000)
                remaining -= amount
                b.rows["transactions"].append({
                    "transaction_id": f"TXN-{b.day:%Y%m%d}-9{n:05d}",
                    "account_id": account["account_id"],
                    "txn_ts": utc_ts(random_ts(rng, b.day)),
                    "txn_type": "withdrawal",
                    "amount": money(amount),
                    "currency": account["currency"],
                    "merchant_category": None,
                    "status": "posted",
                })

        return [
            Scenario("duplicate_transaction_ids", "same transaction_id delivered twice with different amounts",
                     "silver", "fintech_txn_id_unique", duplicate_ids),
            Scenario("malformed_amounts", "amounts like '12,50' or 'N/A' that are not numbers",
                     "bronze", "fintech_bronze_txn_amount_is_numeric", malformed_amounts),
            Scenario("null_transaction_ids", "records without a transaction_id",
                     "bronze", "fintech_bronze_txn_id_present", null_ids),
            Scenario("negative_amounts", "amounts sent with a sign instead of using txn_type",
                     "silver", "fintech_txn_amount_positive", negative_amounts),
            Scenario("amount_above_single_limit", "a single transaction of 25,000 (limit is 10,000)",
                     "silver", "fintech_txn_amount_below_single_limit", above_limit),
            Scenario("invalid_currency", "currency code that is not ISO 4217",
                     "silver", "fintech_txn_currency_supported", invalid_currency),
            Scenario("currency_mismatch", "transaction currency differs from the account currency",
                     "silver", "fintech_txn_currency_matches_account", currency_mismatch),
            Scenario("orphan_transactions", "transactions for an account that does not exist",
                     "silver", "fintech_txn_account_exists", orphan_accounts),
            Scenario("future_timestamps", "transactions dated 30 days in the future",
                     "silver", "fintech_txn_not_in_future", future_ts),
            Scenario("card_payment_without_merchant", "card payments missing their merchant category",
                     "silver", "fintech_card_payment_has_merchant", card_without_merchant),
            Scenario("closed_account_activity", "posted transaction on a closed account",
                     "silver", "fintech_posted_only_on_active_accounts", closed_account_activity),
            Scenario("overdraft_beyond_limit", "withdrawal that pushes an account past its credit limit",
                     "gold", "fintech_no_overdraft_beyond_credit_limit", overdraft),
        ]
