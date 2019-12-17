#!/usr/bin/env bash

spark-submit \
	--class com.zhang.main.RealFeatureStatistic \
	--driver-memory 1G \
	--executor-memory 1g  \
	--deploy-mode cluster \
	--executor-cores 1 \
	--total-executor-cores 1 \
	--master spark://zhangqilongdeMacBook-Air.local:7077 \
	--conf spark.default.parallelism=3 \
	target/BehaviorStatistic-1.0-SNAPSHOT-jar-with-dependencies.jar