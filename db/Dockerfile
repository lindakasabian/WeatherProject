FROM python:3.7-alpine

ADD . /code
WORKDIR /code

RUN apk update && apk add postgresql-dev gcc musl-dev
RUN pip install -r requirements.txt

# Run app.py when container is launched
CMD ["python", "db_upload.py"]
