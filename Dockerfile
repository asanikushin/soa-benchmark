FROM python:3-slim

COPY . .
RUN pip install -r requirements.txt
CMD ["python", "benchmark.py"]