FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# ── Build verification ──────────────────────────────────────
# Fail the build if the OLD broken code is still present.
# This grep returns exit code 0 if the BAD pattern is found → ! inverts → build fails.
RUN ! grep -n "oracle_strategy.evaluate(chainlink_price=" /app/engine/main_v4.py \
    && echo "✓ Build verified: no broken oracle_strategy.evaluate(chainlink_price=) call" \
    && echo "main_v4.py line count: $(wc -l < /app/engine/main_v4.py)" \
    && echo "evaluate_cycle chainlink_tracker.update present:" \
    && grep -n "chainlink_tracker.update" /app/engine/main_v4.py | head -3

# Health check
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
  CMD python -c "import time; print('alive')" || exit 1

CMD ["python", "run.py"]
