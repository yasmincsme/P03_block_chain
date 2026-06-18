FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir web3==6.20.3 py-solc-x
COPY deploy.py .
COPY DroneToken.sol .
COPY audit.py .
CMD ["python3", "deploy.py"]
