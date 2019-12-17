#!/usr/bin/env bash

# 由于本地的kafka只配置了一个partition，所以executor为1

spark-submit \
	--class com.zhang.main.RealFeatureStatistic2 \
	--driver-memory 1G \
	--executor-memory 1g  \
	--deploy-mode cluster \
	--num-executors 1 \
	--executor-cores 2 \
    --master yarn \
	--conf spark.default.parallelism=6 \
    --conf spark.yarn.maxAppAttempts=4 \
	target/BehaviorStatistic-1.0-SNAPSHOT-jar-with-dependencies.jar

