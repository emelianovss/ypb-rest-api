#!/bin/bash

docker build -t ypb-rest-api .
docker run -d -p 8000:8000 --name=rest_api ypb-rest-api