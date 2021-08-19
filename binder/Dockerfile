FROM jupyter/scipy-notebook:lab-3.1.4

USER root

# This will make apt-get install without question
ARG DEBIAN_FRONTEND=noninteractive

# Install updates and required packages
RUN apt-get update && \
    apt-get -y install -q \
    apt-utils \
    sudo \
    git \
    curl \
    wget \
    apt-transport-https \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    zlib1g-dev \
    pkg-config

# Add custom MariaDB repositories
RUN curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup -o mariadb_repo_setup && bash mariadb_repo_setup --skip-maxscale --skip-tools

# Install MariaDB
RUN apt install -y mariadb-server

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER ${NB_USER}
ENV NB_UID ${NB_UID}
ENV HOME /home/${NB_USER}

RUN adduser --disabled-password \
    --gecos "Default user" \
    --uid ${NB_UID} \
    ${NB_USER}

# Make sure the contents of our repo are in ${HOME}
COPY . ${HOME}
RUN chown -R ${NB_UID} ${HOME}

WORKDIR ${HOME}
USER ${USER}

RUN pip install --no-cache --upgrade pip && \
    pip install --no-cache notebook
