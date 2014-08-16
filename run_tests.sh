#!/bin/bash
find . -name '*pyc' | xargs rm -f
export TESTDB=pyutil_testdb
export S3_REPO_CFG=~/.test_s3_repo_cfg
export PYTHONPATH=$PWD:$PYTHONPATH

dropdb $TESTDB
createdb $TESTDB
createuser pyutil 2> /dev/null
cd tests && python -m unittest discover -v . "test*$1*py"
