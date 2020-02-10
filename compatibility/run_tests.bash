#!/usr/bin/env bash

function rebuild_and_compare() {
  comp=$1
  version=$2
  out="out-${comp}-${version}.parquet"

  /buildfile -compression ${comp} -version ${version} -json /data.json -pq /${out}
  /compare -json /data.json -pq /${out}
}


# Create file UNCOMPRESSED / V1
rebuild_and_compare none v1
rebuild_and_compare none v2
rebuild_and_compare gzip v1
rebuild_and_compare gzip v2
rebuild_and_compare snappy v1
rebuild_and_compare snappy v2

