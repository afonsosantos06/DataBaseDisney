import logging
import sqlite3
import re
import os
import populate_db

global DB, DB_FILE

# Caminho robusto para a BD no diretório raiz do projeto
# Resolve sempre relativo a este ficheiro (app/db.py)
DB_FILE = os.path.join(os.path.dirname(__file__), "..", "DisneyDB.db")

DB = dict()


def connect():
    global DB, DB_FILE
    c = sqlite3.connect(DB_FILE, check_same_thread=False)
    # print("connected", c)
    c.row_factory = sqlite3.Row
    DB["conn"] = c
    DB["cursor"] = c.cursor()
    logging.info("Connected to database")


def execute(sql, args=None):
    global DB
    sql = re.sub("\\s+", " ", sql)
    logging.info("SQL: {} Args: {}".format(sql, args))
    return (
        DB["cursor"].execute(sql, args) if args != None else DB["cursor"].execute(sql)
    )


def close():
    global DB
    DB["conn"].close()
