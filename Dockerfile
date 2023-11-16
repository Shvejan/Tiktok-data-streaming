FROM python:3.10.13-alpine3.18


RUN apk update && apk add chromium chromium-chromedriver && apk add libffi-dev

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt
RUN chmod +x /wait-for.sh

CMD ["/bin/sh", "-c", "/wait-for.sh broker:29092 minio:9000 -- python3 scrapper.py"]
