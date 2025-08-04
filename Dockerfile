FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["kopf", "run", "--verbose", "kopf_operator/main.py"]
