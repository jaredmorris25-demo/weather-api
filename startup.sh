#!/bin/bash
# Ensure dependencies are installed in the runtime environment
pip install -r /home/site/wwwroot/requirements.txt

gunicorn app.main:app \
  --workers 2 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120
