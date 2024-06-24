====
cumulus-pre-map-payload-grouping
====
This is the python code for the lambda `cumulus-pre-map-payload-grouping`.
It takes in a list of payload[Granules] and splits them into sub-groups with correct prefixes to be used with `AWS Step Function Maps <https://docs.aws.amazon.com/step-functions/latest/dg/amazon-states-language-map-state.html>`

Required input
====
This function is intended to be used as a cumulus task and this requires ``cumulus_meta`` and ``meta`` inputs from the ``task_config``
::
    "task_config": {
        "cumulus_meta": "{$.cumulus_meta}",
        "meta": "{$.meta}"
    }


Build (as a zip to load to AWS Lambda)
====
`This <https://chariotsolutions.com/blog/post/building-lambdas-with-poetry/>`_ page contains some good info on the overall "building a zip with poetry that's compatible with AWS Lambda".

Auto build script
----
TL;DR ::

    sh ./build.sh;

to run the following commands in order and build the cumulus-pre-map-payload-grouping.zip artifact

Manual build
----
This command creates the ``dist/`` folder::

    poetry build

This command downloads the dependency files from the just created .whl file, along with the lambda_handler function in ``cumulus_publish_cnm/cumulus_publish_cnm.py``, and places them in the ``package`` folder::

    poetry run pip install --upgrade -t package dist/*.whl

Note that **moto** is not being pulled in. I've specifically excluded it via having **moto** as ``tool.poetry.dev-dependencies`` only (via the ``pyproject.toml`` file)

The last command used is::

    cd package ; zip -r ../artifact.zip . -x '*.pyc'

Which zips and creates the ``cumulus-pre-map-payload-grouping.zip`` file, containing all files found in ``package/`` excluding .pyc files

Then upload ``cumulus-pre-map-payload-grouping.zip`` to any location you plan to use it

* Upload directly as a lambda with ``aws lambda update-function-code``
* Upload to your AWS S3 ``lambdas/`` folder so that your Cumulus Terraform Build can use it

Testing & Coverage
====
Run the ``poetry run pytest  --cov cumulus_pre_map_payload_grouping tests -v`` command; you should see a similar output::

    ================================================================ test session starts =================================================================
    platform darwin -- Python 3.9.13, pytest-7.4.4, pluggy-1.5.0 -- /Users/hryeung/Library/Caches/pypoetry/virtualenvs/cumulus-pre-map-payload-grouping-xwquyyR7-py3.9/bin/python
    cachedir: .pytest_cache
    rootdir: /Users/hryeung/PycharmProjects/jpl/cumulus-pre-map-payload-grouping
    plugins: cov-4.1.0
    collected 3 items

    tests/test_basic_functions.py::test_basic_input_output_group_by_default_5 PASSED                                                               [ 33%]
    tests/test_basic_functions.py::test_basic_input_output_group_by_2 PASSED                                                                       [ 66%]
    tests/test_basic_functions.py::test_input_with_replace PASSED                                                                                  [100%]

    ---------- coverage: platform darwin, python 3.9.13-final-0 ----------
    Name                                                                   Stmts   Miss  Cover
    ------------------------------------------------------------------------------------------
    cumulus_pre_map_payload_grouping/__init__.py                               0      0   100%
    cumulus_pre_map_payload_grouping/cumulus_pre_map_payload_grouping.py      27      1    96%
    ------------------------------------------------------------------------------------------
    TOTAL                                                                     27      1    96%


    ================================================================= 3 passed in 1.78s ==================================================================

