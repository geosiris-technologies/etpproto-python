<!--
Copyright (c) 2022-2023 Geosiris.
SPDX-License-Identifier: Apache-2.0
-->
ETP Protocol Implementation
==========

[![License](https://img.shields.io/pypi/l/etpproto)](https://github.com/geosiris-technologies/etpproto-python/blob/main/LICENSE)
[![Documentation Status](https://readthedocs.org/projects/etpproto-python/badge/?version=latest)](https://etpproto-python.readthedocs.io/en/latest/?badge=latest)
[![Python CI](https://github.com/geosiris-technologies/etpproto-python/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/geosiris-technologies/etpproto-python/actions/workflows/ci-tests.yml)
![Python version](https://img.shields.io/pypi/pyversions/etpproto)
[![PyPI](https://img.shields.io/pypi/v/etpproto)](https://badge.fury.io/py/etpproto)
![Status](https://img.shields.io/pypi/status/etpproto)
[![codecov](https://codecov.io/gh/geosiris-technologies/etpproto-python/branch/main/graph/badge.svg)](https://codecov.io/gh/geosiris-technologies/etpproto-python)




Installation
----------

Etpproto-python can be installed with pip : 

```console
pip install etpproto
```

or with poetry: 
```console
poetry add etpproto
```


Developing
----------

First clone the repo from gitlab.

```console
    git clone https://github.com/geosiris-technologies/etpproto-python.git
```

To develop, you should use **[Poetry](https://python-poetry.org/)**.

Install all necessary packages (including for development) with:

```console
    poetry install
```

Then setup the Git pre-commit hook for **[Black](<https://github.com/psf/black>)** and **[Pylint](https://www.pylint.org/)**  by running

```console
    poetry run pre-commit install
```

as the ``rev`` gets updated through time to track changes of different hooks,
simply run

```console
    poetry run pre-commit autoupdate
```
to have pre-commit install the new version.

To bump a new version of the project simply publish a release name 'vX.X.X' with X replaced by your numbers

Test
----------

Run tests with poetry
```console
poetry run pytest -v --cache-clear -rf --cov=etpproto/ --cov-report=term --cov-report=html --maxfail=10
```

Test the code validity : 
```console
poetry run black .
poetry run flake8 .
poetry run mypy etpproto
```