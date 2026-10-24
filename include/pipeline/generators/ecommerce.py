"""E-commerce: customers (CSV), products (JSON), orders (CSV), order_items (JSONL)."""

from __future__ import annotations

import random
from datetime import date, timedelta

from pipeline.generators.base import DomainGenerator, GeneratedBatch, Scenario, money, random_ts, utc_ts

COUNTRIES = ("US", "MX", "CO", "ES", "BR", "DE", "FR")
CATEGORIES = ("electronics", "books", "home", "fashion", "toys")
STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled", "refunded")
WORDS = ("Classic", "Smart", "Eco", "Ultra", "Mini", "Pro", "Vintage", "Travel")
NOUNS = {"electronics": "Speaker", "books": "Novel", "home": "Lamp", "fashion": "Jacket", "toys": "Puzzle"}


def recompute_totals(b: GeneratedBatch) -> None:
    """Keep order totals consistent with their lines so only the intended rule breaks."""
    totals: dict[str, float] = {}
    for item in b.rows["order_items"]:
        line = float(item["quantity"]) * float(item["unit_price"]) - float(item["discount"])
        totals[item["order_id"]] = totals.get(item["order_id"], 0.0) + line
    for order in b.rows["orders"]:
        order["total_amount"] = money(totals.get(order["order_id"], 0.0))


class EcommerceGenerator(DomainGenerator):
    domain = "ecommerce"
    generic_targets = {
        "missing_column": ("orders", "total_amount"),
        "missing_file": "order_items",
        "malformed_record": "products",
        "empty_file": "customers",
    }

    def generate_clean(self, day: date, rows: int, rng: random.Random):
        customers = []
        for i in range(1, max(10, rows // 4) + 1):
            customers.append({
                "customer_id": f"CUS-{i:05d}",
                "email": f"customer{i}@example.com",
                "country": rng.choice(COUNTRIES),
                "signup_date": day - timedelta(days=rng.randint(1, 1500)),
            })

        products = []
        for i in range(1, 41):
            category = CATEGORIES[i % len(CATEGORIES)]
            products.append({
                "sku": f"SKU-{i:04d}",
                "name": f"{rng.choice(WORDS)} {NOUNS[category]} {i}",
                "category": category,
                "unit_price": money(rng.uniform(5, 300) if rng.random() < 0.9 else rng.uniform(300, 900)),
                "active": i <= 36,
            })
        sellable = [p for p in products if p["active"]]

        orders, items = [], []
        for n in range(1, rows + 1):
            customer = rng.choice(customers)
            order_id = f"ORD-{day:%Y%m%d}-{n:05d}"
            for line_number in range(1, rng.randint(1, 4) + 1):
                product = rng.choice(sellable)
                quantity = rng.randint(1, 3)
                gross = quantity * float(product["unit_price"])
                items.append({
                    "order_id": order_id,
                    "line_number": line_number,
                    "sku": product["sku"],
                    "quantity": quantity,
                    "unit_price": product["unit_price"],
                    "discount": money(gross * rng.uniform(0.05, 0.2)) if rng.random() < 0.3 else money(0),
                })
            orders.append({
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "order_ts": utc_ts(random_ts(rng, day)),
                "status": rng.choices(STATUSES, [10, 20, 25, 35, 5, 5])[0],
                "shipping_country": customer["country"],
                "total_amount": None,
            })
        batch = GeneratedBatch(day, {"customers": customers, "products": products, "orders": orders, "order_items": items})
        recompute_totals(batch)
        return batch.rows

    def domain_scenarios(self) -> list[Scenario]:
        def pick(rows, rng, k=3, where=lambda r: True):
            return rng.sample([r for r in rows if where(r)], k)

        def duplicate_orders(b, rng):
            for o in pick(b.rows["orders"], rng):
                b.rows["orders"].append({**o, "status": "cancelled" if o["status"] != "cancelled" else "paid"})

        def invalid_emails(b, rng):
            for c, bad in zip(pick(b.rows["customers"], rng), ["not-an-email", "john@", "@example.com"]):
                c["email"] = bad

        def invalid_status(b, rng):
            for o in pick(b.rows["orders"], rng, 2):
                o["status"] = "lost_in_space"

        def zero_quantity(b, rng):
            for i in pick(b.rows["order_items"], rng):
                i["quantity"], i["discount"] = 0, money(0)
            recompute_totals(b)

        def fractional_quantity(b, rng):
            for i in pick(b.rows["order_items"], rng, 2):
                i["quantity"] = "1.5"

        def excessive_discount(b, rng):
            for i in pick(b.rows["order_items"], rng, 2):
                i["discount"] = money(float(i["unit_price"]) * i["quantity"] + 10)
            recompute_totals(b)

        def total_mismatch(b, rng):
            for o in pick(b.rows["orders"], rng, 2):
                o["total_amount"] = money(float(o["total_amount"]) + 10)

        def unknown_sku(b, rng):
            for i in pick(b.rows["order_items"], rng, 2):
                i["sku"] = "SKU-9999"

        def inactive_product(b, rng):
            sold = b.rows["order_items"][0]["sku"]
            next(p for p in b.rows["products"] if p["sku"] == sold)["active"] = False

        def order_without_items(b, rng):
            b.rows["orders"].append({**b.rows["orders"][0], "order_id": f"ORD-{b.day:%Y%m%d}-99999",
                                     "total_amount": money(0)})

        def bulk_anomaly(b, rng):
            for i in b.rows["order_items"]:
                i["quantity"] = 900
            recompute_totals(b)

        return [
            Scenario("duplicate_order_ids", "same order_id delivered twice with different statuses",
                     "silver", "ecommerce_order_id_unique", duplicate_orders),
            Scenario("invalid_emails", "customer emails that are not valid addresses",
                     "silver", "ecommerce_customer_email_valid", invalid_emails),
            Scenario("invalid_order_status", "order status outside the order lifecycle",
                     "silver", "ecommerce_order_status_valid", invalid_status),
            Scenario("zero_quantity", "order lines with quantity 0",
                     "silver", "ecommerce_item_quantity_valid", zero_quantity),
            Scenario("fractional_quantity", "quantity '1.5' that is not an integer",
                     "bronze", "ecommerce_bronze_order_items_quantity_is_integer", fractional_quantity),
            Scenario("excessive_discount", "discount larger than the line amount",
                     "silver", "ecommerce_item_discount_valid", excessive_discount),
            Scenario("order_total_mismatch", "order total differs from the sum of its lines",
                     "silver", "ecommerce_order_total_matches_items", total_mismatch),
            Scenario("unknown_sku", "order lines for a product not in the catalog",
                     "silver", "ecommerce_item_product_exists", unknown_sku),
            Scenario("inactive_product_sold", "an order line for a discontinued product",
                     "silver", "ecommerce_item_product_active", inactive_product),
            Scenario("order_without_items", "an order header with no lines",
                     "silver", "ecommerce_order_has_items", order_without_items),
            Scenario("bulk_order_anomaly", "every line has quantity 900: each row is valid, the day is not",
                     "gold", "ecommerce_daily_aov_within_bounds", bulk_anomaly),
        ]
