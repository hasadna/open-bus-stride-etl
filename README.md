# Open Bus Stride Enrichment ETL

ETL processing tasks for Stride data enrichment

## Install

Create virtualenv (Python 3.8)

```
python3.8 -m venv venv
```

Upgrade pip

```
venv/bin/pip install --upgrade pip
```

You should have a clone of the following repository in sibling directory:

* `../open-bus-stride-db`: https://github.com/hasadna/open-bus-stride-db

Install dev requirements (this installs above repository as well as this repository as editable for development):

```
venv/bin/pip install -r requirements-dev.txt
```

## Use

Go to open-bus-stride-db repo, pull to update to latest version and follow the 
README to start a local DB and update to latest database migration

Activate the virtualenv

```
. venv/bin/activate
```

See the CLI help message for available tasks

```
open-bus-stride-etl --help
```
