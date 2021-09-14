FROM jupyter/scipy-notebook:lab-3.1.4

USER root

# This will make apt-get install without question
ARG DEBIAN_FRONTEND=noninteractive

RUN sed -Ei 's/^# deb-src /deb-src /' /etc/apt/sources.list

# Install updates and required packages
RUN apt-get update

RUN apt-get -y install -q \
        git \
        build-essential \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        libncurses5-dev \
        gnutls-dev \
        bison \
        pkg-config

RUN apt-get -y upgrade

RUN apt-get -y build-dep -q mariadb-server

RUN git clone -b preview-10.7-MDEV-4742-natural-sort https://github.com/MariaDB/server.git

WORKDIR ./server

RUN cmake . -DMYSQL_MAINTAINER_MODE=OFF -DCONC_WITH_{UNITTEST,SSL}=OFF -DWITH_UNIT_TESTS=OFF -DCMAKE_BUILD_TYPE=Debug -DPLUGIN_{ARCHIVE,TOKUDB,MROONGA,OQGRAPH,ROCKSDB,CONNECT,SPIDER,SPHINX,COLUMNSTORE}=NO -DWITH_EMBEDDED_SERVER=OFF -DWITH_MARIABACKUP=OFF

RUN make -j4

RUN make install

WORKDIR ${HOME}

ENV USER jovyan
ENV NB_UID 1000
ENV HOME /home/${USER}


# Make sure the contents of our repo are in ${HOME}
COPY . ${HOME}
RUN chown -R ${NB_UID} ${HOME}

WORKDIR ${HOME}
USER ${USER}

RUN pip install --no-cache --upgrade pip && \
    pip install --no-cache notebook

RUN mkdir -p ${HOME}/.jupyter
RUN cp binder/mariadb_config.json ${HOME}/.jupyter/

RUN python setup.py develop
RUN python -m mariadb_kernel.install
