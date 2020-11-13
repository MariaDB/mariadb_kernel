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
    build-essential

RUN curl -sS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup -o mariadb_repo_setup && bash mariadb_repo_setup --skip-maxscale --skip-tools

RUN apt install -y mariadb-server

# Download and "silently" install conda from miniforge into ~/miniforge
RUN wget https://github.com/conda-forge/miniforge/releases/download/4.8.3-5/Miniforge-pypy3-4.8.3-5-Linux-x86_64.sh \
	-O ~/miniforge.sh && /bin/bash ~/miniforge.sh -b -p /miniforge

# Add conda to PATH
ENV PATH=/miniforge/bin:${PATH}

# Create python 3.8 environment
RUN conda create -n py38 python=3.8

RUN echo "source /miniforge/etc/profile.d/conda.sh && conda activate py38" >> ~/.bashrc

RUN python3 -m pip install notebook jupyterlab

