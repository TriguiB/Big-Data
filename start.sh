#!/bin/bash

# Start MongoDB
mongod &

# Start Spark Master
/opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master
