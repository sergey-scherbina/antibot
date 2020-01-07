#!/usr/bin/env sh

for i in {1..30}
do
  echo $i
  sed "s/{{time}}/$(date +%s)/g" ../resources/test-data.json | kafka-console-producer --broker-list localhost:9092 --topic test
done