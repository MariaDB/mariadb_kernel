FROM ubuntu:latest

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

RUN curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup -o mariadb_repo_setup && bash mariadb_repo_setup --skip-maxscale --skip-tools

RUN apt install -y mariadb-server

# Download and "silently" install conda from miniforge into ~/miniforge
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
RUN /bin/bash ~/miniconda.sh -b -p ~/miniconda

# Add conda to PATH
ENV PATH=/root/miniconda/bin:${PATH}

# Create python 3.8 environment
RUN conda create -n py38 python=3.8

SHELL ["/bin/bash", "-c"]

RUN echo "source /root/miniconda/etc/profile.d/conda.sh && conda activate py38" >> ~/.bashrc

RUN python3 -m pip install notebook jupyterlab

