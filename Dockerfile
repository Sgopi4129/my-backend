FROM python:3.8.10

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt
COPY app.py .
COPY data.json .

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "app:app"]