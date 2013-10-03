#!/bin/bash
find . -name '*pyc' | xargs rm -f
export TESTDB=pyutil_testdb
export S3CACHE_CONFIG=.test_config

dropdb pyutil_testdb
createdb pyutil_testdb
echo "create extension hstore; commit"  | psql -d pyutil_testdb

python -m unittest discover -fv . "test*$1*py"
