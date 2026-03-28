"""
Settlement Logic Fix
=====================
The CRITICAL settlement bug: 15/112 trades (13.4%) had inverted won/lost.

Root cause: settlement code used `direction == outcome` instead of
`side + outcome` to determine won/lost.

CORRECT LOGIC:
  side=YES + outcome=UP   → won=True
  side=YES + outcome=DOWN → won=False
  side=NO  + outcome=DOWN → won=True
  side=NO  + outcome=UP   → won=False

This file provides the corrected settlement function and
dispute protection logic. Apply these changes to the existing
settlement_watcher.py.
"""


def compute_settlement(side: str, outcome: str) -> dict:
    """
    Compute whether a trade won or lost based on side and outcome.

    This is the SINGLE SOURCE OF TRUTH for settlement logic.
    Every settlement path (real-time watcher, backfill loop,
    paper settlement) must call this function.

    Args:
        side: "YES" or "NO" — which token we bought
        outcome: "UP" or "DOWN" — how the market resolved

    Returns:
        dict with keys: won (bool), reason (str)

    Raises:
        ValueError if side or outcome are invalid
    """
    side = side.upper().strip()
    outcome = outcome.upper().strip()

    if side not in ("YES", "NO"):
        raise ValueError(f"Invalid side: '{side}' (must be YES or NO)")
    if outcome not in ("UP", "DOWN"):
        raise ValueError(f"Invalid outcome: '{outcome}' (must be UP or DOWN)")

    won = (
        (side == "YES" and outcome == "UP") or
        (side == "NO" and outcome == "DOWN")
    )

    return {
        "won": won,
        "reason": f"side={side} + outcome={outcome} → {'WIN' if won else 'LOSS'}",
    }


def compute_pnl(
    won: bool,
    fill_price: float,
    size_usd: float,
    taker_fee_rate: float = 0.0156,
) -> dict:
    """
    Compute P&L from a settled trade.

    Uses the ACTUAL fill price (what we paid), not the hypothetical.

    Args:
        won: True if the trade won
        fill_price: what we actually paid per share
        size_usd: total position in USD
        taker_fee_rate: fee rate (0 for maker, 0.0156 for taker)

    Returns:
        dict with keys: gross_pnl, fee, net_pnl, shares
    """
    if fill_price <= 0 or fill_price >= 1.0:
        return {"gross_pnl": 0, "fee": 0, "net_pnl": 0, "shares": 0}

    shares = size_usd / fill_price

    if won:
        gross_pnl = (1.0 - fill_price) * shares
    else:
        gross_pnl = -fill_price * shares

    fee = size_usd * taker_fee_rate
    net_pnl = gross_pnl - fee

    return {
        "gross_pnl": round(gross_pnl, 4),
        "fee": round(fee, 4),
        "net_pnl": round(net_pnl, 4),
        "shares": round(shares, 4),
    }


def check_settlement_dispute(clob_outcome: str, gamma_outcome: str) -> dict:
    """
    Check for oracle dispute — when CLOB and Gamma APIs disagree.

    During black swan events, the first proposed resolution may be wrong.
    If the two APIs disagree, flag it and don't settle.

    Args:
        clob_outcome: outcome from CLOB API
        gamma_outcome: outcome from Gamma API

    Returns:
        dict with: agrees (bool), clob, gamma, should_settle (bool)
    """
    clob = clob_outcome.upper().strip() if clob_outcome else ""
    gamma = gamma_outcome.upper().strip() if gamma_outcome else ""

    agrees = clob == gamma and clob in ("UP", "DOWN")

    return {
        "agrees": agrees,
        "clob": clob,
        "gamma": gamma,
        "should_settle": agrees,
        "reason": (
            "CLOB and Gamma agree" if agrees
            else f"DISPUTE: CLOB={clob} vs Gamma={gamma} — waiting for consensus"
        ),
    }
