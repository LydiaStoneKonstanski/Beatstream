FROM python:3.12-slim
LABEL authors="Chris, Deepa, Lydia"

WORKDIR /app

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

EXPOSE 5000

CMD ["python3","app.py"]
