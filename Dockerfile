FROM python:3.14-slim

WORKDIR /data-board

COPY requirements.txt .

RUN pip install -r requirements.txt
