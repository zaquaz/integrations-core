#!/bin/bash

source utils/bash/ci.sh
NAME='dd-test-memcache'
stop-docker $NAME

VERSION=${FLAVOR_VERSION-1.4.22}

docker run -d --name $NAME -p 11212:11211 memcached:$VERSION


CONN=$("$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/mc_conn_tester.pl -s localhost -p 11212 -c 1 --timeout 1)
NUM=0

until [[ $CONN =~ "loop: (timeout: 1) (elapsed:" ]]; do
  if [ NUM -eq 11 ]; then
    echo 'memcache has not come up'
    exit 1
  fi
  CONN=$("$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/mc_conn_tester.pl -s localhost -p 11212 -c 1 --timeout 1)
  NUM=$(( NUM + 1 ))
done
