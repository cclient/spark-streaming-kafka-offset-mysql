#!/bin/sh
sbt package
docker build -t cuidapeng/spark-offset:$1 ./
docker push cuidapeng/spark-offset:$1