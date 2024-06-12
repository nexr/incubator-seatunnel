FROM openjdk:8-slim-buster AS build
RUN apt update -y && dpkg --purge --force-depends ca-certificates-java
RUN apt install -y ca-certificates-java
RUN apt install -y maven
RUN apt install -y liblzo2-dev

RUN mkdir /seatunnel
COPY . /seatunnel/
WORKDIR /seatunnel
RUN ./mvnw install -Dmaven.test.skip -Drevision=2.3.3
