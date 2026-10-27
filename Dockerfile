FROM python:3.12-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml ./
COPY dkv/ dkv/
RUN pip install --no-cache-dir ".[server]" && useradd --create-home dkv && mkdir /data && chown dkv /data
USER dkv
ENV DKV_DATA=/data
EXPOSE 8000
CMD ["uvicorn", "dkv.server:app", "--host", "0.0.0.0", "--port", "8000"]
