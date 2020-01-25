FROM python:3.7-alpine

ADD . /code
WORKDIR /code

RUN apk update && apk add postgresql-dev gcc musl-dev
RUN pip install -r requirements.txt

EXPOSE 5432

CMD ["python", "db_upload.py"]
