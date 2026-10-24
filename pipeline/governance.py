"""Column classification and the rules for what may be published."""

from __future__ import annotations

import re

# Classes whose values never leave the pipeline, not even as counts.
WITHHELD = {"personal", "sensitive"}
# Classes published as category counts.
COUNTED = {"categorical", "metadata"}


def classify(column_name: str, rules: list[dict]) -> str:
    for rule in rules:
        if re.search(rule["match"], column_name):
            return rule["class"]
    return "categorical"


def suppress(counts: list[tuple[str, int]], min_group: int, max_categories: int) -> list[dict]:
    """Fold groups smaller than min_group, and anything past max_categories, into one bucket.

    The folded bucket is itself dropped when it is smaller than min_group, so a single
    rare answer can't be recovered by subtracting the visible groups from the total.
    """
    shown, other = [], 0
    for value, count in sorted(counts, key=lambda c: (-c[1], c[0])):
        if count >= min_group and len(shown) < max_categories:
            shown.append({"value": value, "count": count})
        else:
            other += count
    if other >= min_group:
        shown.append({"value": f"Other (groups < {min_group})", "count": other, "folded": True})
    elif other:
        shown.append({"value": "Suppressed", "count": None, "folded": True})
    return shown
