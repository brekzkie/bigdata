FROM python:3.10-slim

WORKDIR /app
RUN apt-get update && \
    apt-get install -y ca-certificates openssl curl && \
    update-ca-certificates

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

CMD ["python", "-u", "main.py"]
