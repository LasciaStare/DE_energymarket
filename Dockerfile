FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 app && mkdir /lake && chown app /lake
COPY api.py pipeline.py metrics.yml dashboard.html ./
USER app
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
