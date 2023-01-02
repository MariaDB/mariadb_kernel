FROM jupyter/scipy-notebook
USER root

RUN apt update
RUN apt install -y mariadb-server
RUN pip install mariadb_kernel
RUN python3 -m mariadb_kernel.install
