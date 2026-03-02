FROM python:3.12-slim

WORKDIR /app

COPY utils.py  .
COPY utils_log.py .
COPY node.py   .
COPY store.py  .
COPY client.py .

RUN mkdir -p /app/data

CMD ["python", "-u", "node.py"]
