FROM python:3.12-slim
WORKDIR /app
COPY *.whl /tmp/
RUN pip install --no-cache-dir /tmp/*.whl
COPY consumer.py .
CMD ["python", "consumer.py"]
