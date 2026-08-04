FROM python:3.12-slim
RUN apt-get update && apt-get install -y cron && rm -rf /var/lib/apt/lists/*
RUN pip install requests
WORKDIR /app
COPY sync_off.py .
RUN echo "0 3 1,15 * * cd /app && python3 sync_off.py >> /var/log/sync.log 2>&1" > /etc/cron.d/sync-cron
RUN chmod 0644 /etc/cron.d/sync-cron
RUN crontab /etc/cron.d/sync-cron
RUN touch /var/log/sync.log
CMD cron -f
