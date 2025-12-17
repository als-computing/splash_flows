FROM prefecthq/prefect:3.4.2-python3.13

WORKDIR /app
COPY ./requirements.txt /tmp/

RUN pip install -U pip &&        pip install -r /tmp/requirements.txt

COPY . /app/
RUN pip install -e .