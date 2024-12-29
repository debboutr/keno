FROM python:3.12-slim

ENV TZ="America/Los_Angeles"

WORKDIR /app

COPY requirements.txt /app

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
