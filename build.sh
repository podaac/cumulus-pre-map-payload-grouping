#!/bin/sh
poetry build
poetry run pip install --upgrade -t package dist/*.whl
cd package ; zip -r ../artifact.zip . -x '*.pyc'
cd ..
code_version=`poetry version | awk '{print $2}'`
code_name=`poetry version | awk '{print $1}'`
cp artifact.zip $code_name-$code_version.zip
echo \*\* $code_name-$code_version.zip created \*\*