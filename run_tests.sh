#!/bin/bash
find . -name '*pyc' | xargs rm -f
python -m unittest discover -fv . "test*$1*py"
