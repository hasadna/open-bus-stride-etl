# Open Bus Stride Enrichment ETL

ETL processing tasks for Stride data enrichment

See [our contributing docs](https://github.com/hasadna/open-bus-pipelines/blob/main/CONTRIBUTING.md) if you want to suggest changes to this repository.

## Development using the Docker Compose environment

This is the easiest option to start development, follow these instructions: https://github.com/hasadna/open-bus-pipelines/blob/main/README.md#stride-etl

For local development, see the additional functionality section: `Develop stride-etl from a local clone`

## Development using local Python interpreter

It's much easier to use the Docker Compose environment, but the following can be
refferd to for more details regarding the internal processes and for development
using your local Python interpreter. 

### Install

Create virtualenv (Python 3.8)

```
python3.8 -m venv venv
```

Upgrade pip

```
venv/bin/pip install --upgrade pip
```

You should have a clone of the following repositories in sibling directories:

* `../open-bus-stride-db`: https://github.com/hasadna/open-bus-stride-db
* `../open-bus-stride-api`: https://github.com/hasadna/open-bus-stride-api
* `../open-bus-stride-client`: https://github.com/hasadna/open-bus-stride-client

Install dev requirements (this installs above repository as well as this repository as editable for development):

```
venv/bin/pip install -r requirements-dev.txt
```

### Use

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
