#!/bin/bash

source utils/bash/ci.sh
NAME='dd-test-memcache'
stop-docker $NAME

VERSION=${FLAVOR_VERSION-1.4.22}

docker run -d --name $NAME -p 11211:11211 memcached:$VERSION
