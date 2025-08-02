FROM python:3.13-slim

# Define build argument
ARG requirements=""

ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install uv

RUN uv pip install --system ${requirements}

COPY resources/source.py resources/source.py
COPY resources/template.py resources/template.py