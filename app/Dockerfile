FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pyarrow psycopg2-binary

COPY *.py ./

CMD ["sh", "-c", "python server.py"]
