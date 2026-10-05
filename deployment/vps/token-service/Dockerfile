# Guest token service: Python stdlib only, no packages installed.
FROM python:3.12-alpine

COPY server.py /app/server.py
USER 65534:65534
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
EXPOSE 8080
CMD ["python", "/app/server.py"]
