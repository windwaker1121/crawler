#!/bin/bash
DATABASE_PATH="./db-data"
PGADMIN_PATH="./pgadmin-data"
if ! [ -d $DATABASE_PATH ]; then
    echo "Directory is not exsist $DATABASE_PATH, auto create"
    mkdir -m 777 $DATABASE_PATH
fi
if ! [ -d $PGADMIN_PATH ]; then
    echo "Directory is not exsist $PGADMIN_PATH, auto create"
    mkdir -m 777 $PGADMIN_PATH
fi
# ./env_generater.sh > .env.dev
docker compose up --build