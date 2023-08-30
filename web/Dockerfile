FROM python:3
ENV PYTHONUNBUFFERED 1
RUN mkdir /code
WORKDIR /code
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    rm -rf .venv
COPY . /code/
RUN poetry install
# CMD python run.py
CMD gunicorn --bind 0.0.0.0:8080 -w 3 run:app