FROM python:3.12-slim

WORKDIR /app

COPY confluent_kafka-2.11.1-cp312-cp312-manylinux_2_28_x86_64.whl /tmp/

RUN pip install --no-cache-dir /tmp/confluent_kafka-2.11.1-cp312-cp312-manylinux_2_28_x86_64.whl

COPY producer.py .

CMD ["python", "producer.py"]

