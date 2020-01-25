FROM python:3.7-alpine

ADD . /code
WORKDIR /code

RUN apk update && apk add postgresql-dev gcc musl-dev linux-headers g++

RUN pip install numpy cython pandas

RUN pip install -r requirements.txt

CMD ["python", "-u", "app.py"]

