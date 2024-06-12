#!/bin/bash

docker build -t harbor.nexr.kr/nep/seatunnel-pre-installed:2.3.3 .
docker push harbor.nexr.kr/nep/seatunnel-pre-installed:2.3.3
