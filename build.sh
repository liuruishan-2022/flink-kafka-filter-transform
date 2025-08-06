#!/bin/sh
commitId=$(git log -n 1 --pretty=format:"%H")
short_commit_id=${commitId:0:10}
VERSION="v1.0.0-${short_commit_id}"

echo $VERSION

cargo build --release
docker build -t xwharbor.wxchina.com/cpaas/component/flink-kafka-filter-transform:$VERSION .

docker push xwharbor.wxchina.com/cpaas/component/flink-kafka-filter-transform:$VERSION