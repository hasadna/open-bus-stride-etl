#!/usr/bin/env bash

sudo apt-get install -y python3-venv &&\

venv/bin/python -m pip install --upgrade pip &&\
venv/bin/python -m pip install --upgrade setuptools wheel &&\
venv/bin/python -m pip install -r requirements.txt