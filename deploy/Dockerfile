FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir requests websocket-client
COPY memecoin_bot.py ./
COPY spidey ./spidey
# state.json, backups/ and logs live in /data (a volume) so they survive container rebuilds
WORKDIR /data
ENV PYTHONUNBUFFERED=1 SPIDEY_HEALTH_PORT=8080
HEALTHCHECK --interval=60s --timeout=5s --start-period=120s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/',timeout=4).status==200 else 1)"
CMD ["python", "/app/memecoin_bot.py"]
