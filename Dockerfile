FROM tensorflow/tensorflow:1.3.0-py3

RUN  pip install --upgrade pip\
    && apt-get update -y\
    && apt-get install g++\
    && apt-get install unixodbc-dev -y\
    && apt-get install libpq-dev -y\
    && apt-get install python3-dev -y \
    && apt-get install vim -y \
    && apt-get install host -y \
    && apt-get install ssh -y \
    && apt-get install git -y \
    && apt-get autoremove -yqq --purge \
    && apt-get clean\
    && pip install db-utils

COPY db-utils_example.ipynb /notebooks/db-utils_example.ipynb
COPY db_utils /usr/local/lib/python3.5/dist-packages/db_utils
COPY tests /root/tests