ARG build_for=linux/amd64

FROM --platform=$build_for python:3.9.9-slim-bullseye as base

ARG dbt_bigquery_ref=dbt-bigquery@v1.0.0

RUN apt-get update \
  && apt-get dist-upgrade -y \
  && apt-get install -y --no-install-recommends \
    git \
    ssh-client \
    software-properties-common \
    make \
    build-essential \
    ca-certificates \
    libpq-dev \
  && apt-get clean \
  && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

WORKDIR /usr/app/dbt
VOLUME /usr/app
ENTRYPOINT [ "dbt" ]

FROM base as dbt-bigquery
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/${dbt_bigquery_ref}#egg=dbt-bigquery"

RUN python -m pip install --no-cache "git+https://github.com/dbt-labs/${dbt_bigquery_ref}#egg=dbt-bigquery"