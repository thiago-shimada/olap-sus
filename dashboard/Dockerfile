FROM python:3.11-slim

WORKDIR /app

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
build-essential \
gcc \
&& rm -rf /var/lib/apt/lists/*


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

ENV PORT=8050
EXPOSE 8050

CMD ["gunicorn", "app:server", "-b", "0.0.0.0:8050", "-w", "4", "--timeout", "120"]