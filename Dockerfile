FROM puckel/docker-airflow:1.10.9
USER root
RUN apt-get update
RUN apt-get -y install \
    python3-pip \
    unixodbc-dev \
    curl \
    ca-certificates \
    iputils-ping \
    net-tools \
    traceroute \
    p7zip-full
RUN apt-get update && apt-get install -y gnupg2

# Bloomberg API library
ENV APP /app
RUN mkdir $APP
WORKDIR $APP
COPY . .
ENV BLPAPI_ROOT /app/lib/blpapi_cpp_3.14.3.1
ENV LD_LIBRARY_PATH /app/lib/blpapi_cpp_3.14.3.1/Linux
ENV PATH "$PATH:/app/lib/blpapi_cpp_3.14.3.1"
RUN pip3 install --index-url=https://bcms.bloomberg.com/pip/simple/ -Iv blpapi==3.14.0

COPY requirements.txt .
RUN pip3 install -r requirements.txt
RUN pip3 install pdblp

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17
RUN ACCEPT_EULA=Y apt-get -y install mssql-tools
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"
COPY airflow.cfg /usr/local/airflow/airflow.cfg