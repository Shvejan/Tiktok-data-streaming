FROM python:3.10.13-alpine3.18


RUN apk update && apk add chromium chromium-chromedriver && apk add libffi-dev

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

CMD ["python3", "producer.py"]
