FROM ubuntu:18.04

RUN apt-get update
RUN apt-get -y install \
    iputils-ping \
    curl \
    jq \
    apt-transport-https \
    ca-certificates \
    software-properties-common
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"
RUN apt-get -y install docker-ce
ADD scheduler_healthcheck.sh .
RUN ["chmod", "777", "./scheduler_healthcheck.sh"]
