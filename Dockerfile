# Pulled June 16, 2021
FROM python:3.8@sha256:c7706b8d1b1e540b9dd42ac537498d7f3138e4b8b89fb890b2ee4d2c0bccc8ea
RUN pip install --upgrade pip
WORKDIR /srv
COPY stride-db-latest-commit.txt ./
RUN cat stride-db-latest-commit.txt &&\
    pip install -r https://raw.githubusercontent.com/hasadna/open-bus-stride-db/main/requirements.txt &&\
    git clone https://github.com/hasadna/open-bus-stride-db.git &&\
    pip install -e open-bus-stride-db
COPY requirements.txt ./open-bus-stride-etl/requirements.txt
RUN pip install -r open-bus-stride-etl/requirements.txt
COPY setup.py ./open-bus-stride-etl/setup.py
COPY open_bus_stride_etl ./open-bus-stride-etl/open_bus_stride_etl
RUN pip install -e open-bus-stride-etl
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["open-bus-stride-etl"]
CMD ["--help"]