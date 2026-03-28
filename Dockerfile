FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Health check
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
  CMD python -c "import time; print('alive')" || exit 1

CMD ["python", "run.py"]
