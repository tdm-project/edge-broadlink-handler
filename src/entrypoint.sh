#!/bin/sh

cd ${APP_HOME}
. venv/bin/activate
python src/broadlink-handler.py $@
