# Lightweight Linux dev environment for running project scripts
FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
