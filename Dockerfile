FROM tensorflow/tensorflow:1.3.0-gpu-py3

RUN  pip install --upgrade pip -yqq\
	&& apt-get install update -yqq\
	&& apt-get install upgrade -yqq\
	&& curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
	&& curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list \
	&& apt-get install unixodbc-dev -y \
	&& sudo apt-get update \
	&& sudo ACCEPT_EULA=Y apt-get install msodbcsql17 \
	&& apt-get install unixodbc-dev \
	&& apt-get install libpq-dev \
	&& apt-get install python3-dev -y \
	&& apt-get install vim -y \
    && apt-get install host -y \
    && apt-get install ssh -y \
    && apt-get install git -y \