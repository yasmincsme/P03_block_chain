FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
ENV TERM=xterm-256color
WORKDIR /app
RUN pip install --no-cache-dir web3==6.20.3
COPY monitor.py .
CMD ["python3", "monitor.py"]
