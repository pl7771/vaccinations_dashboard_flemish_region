# FROM ubuntu:16.04
FROM python:3.8-slim-buster

# apt-get and system utilities
RUN apt-get update && apt-get install -y \
    curl apt-utils apt-transport-https debconf-utils gcc build-essential g++ \
    && rm -rf /var/lib/apt/lists/*
  
# adding custom MS repository
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

# install SQL Server drivers
# RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql unixodbc-dev
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 unixodbc-dev

# install system dependencies
# RUN apt-get update \
#   && apt-get -y install gcc \
#   && apt-get -y install g++ \
#   && apt-get -y install unixodbc unixodbc-dev \
#   && apt-get clean

# Do not cache Python packages
ENV PIP_NO_CACHE_DIR=yes

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/code/"

# Initializing new working directory
WORKDIR /code

# Transferring the code and essential data
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini
COPY logger.ini ./logger.ini
COPY Pipfile ./Pipfile
COPY Pipfile.lock ./Pipfile.lock
COPY main.py ./main.py

RUN pip install pipenv
RUN pipenv install --ignore-pipfile --system