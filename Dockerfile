FROM python:3.12-slim
RUN pip install requests
WORKDIR /app
COPY sync_off.py .
CMD ["sleep", "infinity"]