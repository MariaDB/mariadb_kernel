FROM jupyter/scipy-notebook:lab-3.1.4

USER root

# This will make apt-get install without question
ARG DEBIAN_FRONTEND=noninteractive

# Install updates and required packages
RUN apt-get update && \
    apt-get -y install -q \
    curl libaio1 libtinfo5 libncurses5

RUN curl -sS https://ci.mariadb.org/18393/amd64-debian-9-bintar-initd/mariadb-10.7.0-linux-x86_64.tar.gz \
    | tar -zxf - -C /usr/local/ --strip-components=1  --exclude '*/include/mysql/server' \
    --exclude '*/mysql-test' --exclude '*/sql-bench' --exclude '*/man' --exclude '*/support-files'

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
