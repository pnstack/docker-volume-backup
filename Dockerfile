# Stage 1: Build 
FROM python:3.10.7-slim-buster as builder

WORKDIR /app

COPY requirements.txt .

# Install build dependencies and libraries
RUN pip install --no-cache-dir -r requirements.txt


# Stage 2: Production
FROM python:3.10.7-slim-buster as production

WORKDIR /app

COPY . .

# Copy only the compiled result from previous stage
COPY --from=builder /app .

CMD ["python", "main.py"]