#!/usr/bin/env bash

spark-submit \
	--class com.zhang.main.RealFeatureStatistic2 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--deploy-mode cluster \
	--executor-cores 2 \
	--total-executor-cores 2 \
	--master spark://zhangqilongdeMacBook-Air.local:7077 \
	--conf spark.default.parallelism=6 \
	target/BehaviorStatistic-1.0-SNAPSHOT-jar-with-dependencies.jar
