#!/bin/bash
find . -name '*pyc' | xargs rm -f
export TESTDB=pyutil_testdb
export S3_REPO_CFG=~/.test_s3_repo_cfg
export PYTHONPATH=$PWD

dropdb pyutil_testdb
createdb pyutil_testdb
cd tests && python -m unittest discover -fv . "test*$1*py"
