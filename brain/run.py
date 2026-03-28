"""
run.py — Brain runner. Executes specified tier(s).

Usage:
  python -m brain.run --tier 1        # Market intelligence
  python -m brain.run --tier 2        # Quant analysis
  python -m brain.run --tier 3        # Strategy discovery
  python -m brain.run --tier all      # All tiers in sequence
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(name)-10s | %(message)s", stream=sys.stdout)
logger = logging.getLogger("brain")


def main():
    parser = argparse.ArgumentParser(description="Oracle Bot v4 Brain")
    parser.add_argument("--tier", choices=["1", "2", "3", "all"], default="all")
    args = parser.parse_args()

    try:
        if args.tier in ("1", "all"):
            from brain.tier1_intelligence import run as run_t1
            run_t1()

        if args.tier in ("2", "all"):
            from brain.tier2_quant import run as run_t2
            run_t2()

        if args.tier in ("3", "all"):
            from brain.tier3_discovery import run as run_t3
            run_t3()

    except Exception as e:
        logger.error(f"Brain crashed: {e}", exc_info=True)
        try:
            from brain.base import BrainBase
            BrainBase().alert(f"Brain CRASHED: {str(e)[:150]}", "ERROR")
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
