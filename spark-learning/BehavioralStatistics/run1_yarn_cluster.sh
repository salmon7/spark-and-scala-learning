#!/usr/bin/env bash

spark-submit \
	--class com.zhang.main.RealFeatureStatistic \
	--driver-memory 1G \
	--executor-memory 1g  \
	--deploy-mode cluster \
	--num-executors 1 \
	--executor-cores 2 \
	--master yarn \
	--conf spark.default.parallelism=4 \
	target/BehaviorStatistic-1.0-SNAPSHOT-jar-with-dependencies.jar
