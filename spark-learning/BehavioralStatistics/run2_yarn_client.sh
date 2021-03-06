#!/usr/bin/env bash

spark-submit \
	--class com.zhang.main.RealFeatureStatistic2 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--deploy-mode client \
	--num-executors 1 \
	--executor-cores 2 \
	--master yarn \
	--conf spark.default.parallelism=4 \
	--conf spark.yarn.maxAppAttempts=4 \
	target/BehaviorStatistic-1.0-SNAPSHOT-jar-with-dependencies.jar
