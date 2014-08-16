#!/bin/bash
dropdb testdb
dropuser s3repo
createuser s3repo
createdb testdb -O s3repo
psql -Xq -U s3repo -d testdb -h localhost -v ON_ERROR_STOP=1 -f postgres/install.sql
