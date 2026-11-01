"""Archive or restore a people source (config/people_sources_state.yml).

    python pipeline/people_sources.py archive mentee_2024 --reason "replaced by new consolidated" --actor someone
    python pipeline/people_sources.py restore mentee_2024 --actor someone
    python pipeline/people_sources.py list

Archived sources (source tabs or pairing sheets from config/people.yml) are not read by the people job, so they drop
out of people gold and every page until restored. Run from GitHub Actions by .github/workflows/people-sources.yml.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "config" / "people_sources_state.yml"
CONFIG = ROOT / "config" / "people.yml"
HEADER = """# People sources that are archived: not read, so they drop out of people gold, the tree, every count and the
# lineage until restored. Managed by .github/workflows/people-sources.yml (Archive / Restore buttons on People ->
# Lineage and People -> Sources), which commits each change here with who, when and why. Restoring a source
# removes its entry; the next people run reads it again. Keys are the source keys in config/people.yml.
"""


def load_state(path: Path | None = None) -> dict:
    path = path or STATE
    if not path.exists():
        return {"archived": {}}
    data = yaml.safe_load(path.read_text()) or {}
    data["archived"] = data.get("archived") or {}
    return data


def known_keys(config_path: Path | None = None) -> dict[str, str]:
    config_path = config_path or CONFIG
    cfg = yaml.safe_load(config_path.read_text())["people"]
    return {s["key"]: s.get("title", s["key"]) for s in cfg.get("sources", []) + cfg.get("pairings", [])}


def save_state(state: dict, path: Path | None = None) -> None:
    path = path or STATE
    body = yaml.safe_dump({"archived": state["archived"]}, sort_keys=True, allow_unicode=True, default_flow_style=False)
    path.write_text(HEADER + body)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["archive", "restore", "list"])
    ap.add_argument("source", nargs="?")
    ap.add_argument("--reason", default="")
    ap.add_argument("--actor", default="unknown")
    args = ap.parse_args(argv)
    state, keys = load_state(), known_keys()
    if args.action == "list":
        for k, title in keys.items():
            a = state["archived"].get(k)
            print(f"{'ARCHIVED' if a else 'active  '}  {k:32} {title}" + (f"  ({a.get('at')} by {a.get('by')}: {a.get('reason')})" if a else ""))
        return 0
    if args.source not in keys:
        print(f"Unknown source '{args.source}'. Known: {', '.join(sorted(keys))}", file=sys.stderr)
        return 2
    if args.action == "archive":
        if args.source in state["archived"]:
            print(f"{args.source} is already archived; nothing to do.")
            return 0
        state["archived"][args.source] = {"at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "by": args.actor,
                                          "reason": args.reason.strip() or "no reason given", "title": keys[args.source]}
        print(f"Archived {args.source} ({keys[args.source]}).")
    else:
        if args.source not in state["archived"]:
            print(f"{args.source} is not archived; nothing to do.")
            return 0
        del state["archived"][args.source]
        print(f"Restored {args.source} ({keys[args.source]}).")
    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
