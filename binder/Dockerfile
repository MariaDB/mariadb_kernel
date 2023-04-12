FROM jupyter/scipy-notebook:ubuntu-22.04

USER root

# This will make apt-get install without question
ARG DEBIAN_FRONTEND=noninteractive

# Add custom MariaDB repositories
RUN echo 'deb [trusted=yes] https://ci.mariadb.org/37014/amd64-ubuntu-2204-deb-autobake/debs ./' > /etc/apt/sources.list.d/mariadb.list

# Install MariaDB
RUN apt-get update -y && apt-get install -y mariadb-server mariadb-plugin-hashicorp-key-management

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
