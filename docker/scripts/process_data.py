#!/usr/bin/env python3
"""Sample data processing script for distributed scheduler demo.

Usage:
    python3 process_data.py --input <format> [--rows N]

Generates and "processes" synthetic data to demonstrate real job execution.
"""

import argparse
import json
import random
import sys
import time


def generate_csv_data(rows):
    """Generate and process synthetic CSV-like data."""
    print(f"Processing {rows} rows of CSV data...")
    total = 0
    for i in range(rows):
        value = random.uniform(1.0, 100.0)
        total += value
    avg = total / rows
    print(f"Processed {rows} rows")
    print(f"  Sum:     {total:.2f}")
    print(f"  Average: {avg:.2f}")
    print(f"  Min est: ~1.00")
    print(f"  Max est: ~100.00")
    return {"rows": rows, "sum": round(total, 2), "average": round(avg, 2)}


def generate_json_data(rows):
    """Generate and process synthetic JSON records."""
    print(f"Processing {rows} JSON records...")
    categories = ["electronics", "clothing", "food", "books", "toys"]
    cat_totals = {c: 0.0 for c in categories}

    for _ in range(rows):
        cat = random.choice(categories)
        amount = random.uniform(5.0, 500.0)
        cat_totals[cat] += amount

    print("Category breakdown:")
    for cat, total in sorted(cat_totals.items()):
        print(f"  {cat}: ${total:.2f}")

    return {"rows": rows, "categories": {k: round(v, 2) for k, v in cat_totals.items()}}


def main():
    parser = argparse.ArgumentParser(description="Process synthetic data")
    parser.add_argument("--input", required=True, choices=["csv", "json"],
                        help="Input data format")
    parser.add_argument("--rows", type=int, default=1000,
                        help="Number of rows to process (default: 1000)")
    args = parser.parse_args()

    start = time.time()

    if args.input == "csv":
        result = generate_csv_data(args.rows)
    else:
        result = generate_json_data(args.rows)

    elapsed = time.time() - start

    print(f"\nCompleted in {elapsed:.3f}s")
    print(f"Result: {json.dumps(result)}")


if __name__ == "__main__":
    main()
