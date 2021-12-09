FROM python:3.8-slim
WORKDIR /app
COPY ./meters /app
RUN chmod -R 755 /app/*
RUN cd /app && apt-get update && \
    pip install -r requirements.txt
