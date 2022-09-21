ETP Protocol Implementation
==========

[![License](https://img.shields.io/pypi/l/etpproto-python)](https://github.com/geosiris-technologies/etpproto-python/blob/main/LICENSE)
[![Documentation Status](https://readthedocs.org/projects/etpproto-python/badge/?version=latest)](https://etpproto-python.readthedocs.io/en/latest/?badge=latest)
[![Python CI](https://github.com/geosiris-technologies/etpproto-python/actions/workflows/ci-tests.yml/badge.svg)](https://github.com/geosiris-technologies/etpproto-python/actions/workflows/ci-tests.yml)
![Python version](https://img.shields.io/pypi/pyversions/etpproto-python)
[![PyPI](https://img.shields.io/pypi/v/etpproto-python)](https://badge.fury.io/py/etpproto-python)
![Status](https://img.shields.io/pypi/status/etpproto-python)
[![codecov](https://codecov.io/gh/bp/etpproto-python/branch/master/graph/badge.svg)](https://codecov.io/gh/bp/etpproto-python)




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

To bump a new version of the project simply run: 
```console
    poetry version [patch, minor, major]
```
You must choose between the semver rules [patch, minor, major]