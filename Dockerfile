FROM python:3.13-slim
WORKDIR /app
COPY cartlab ./cartlab
COPY web ./web
COPY run.py .
ENV PYTHONUNBUFFERED=1
EXPOSE 8760
CMD ["python", "run.py", "--host", "0.0.0.0", "--data-dir", "/data"]
