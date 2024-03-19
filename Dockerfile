FROM python:3.10.7-slim-buster

WORKDIR /app

RUN mkdir -p /tmp/outputs

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]