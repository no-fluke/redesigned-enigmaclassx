FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY classx_telegram_bot.py .

# Expose health-check / self-ping port
# Render injects $PORT automatically; default is 8080
EXPOSE 8080

CMD ["python", "classx_telegram_bot.py"]
