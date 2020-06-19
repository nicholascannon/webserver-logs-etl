#!/usr/bin/env python3
#
# Simple script to provision a database for this data pipeline project.
#
# Written by Nicholas Cannon
import sys
import os
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, Text
import logging

logging.basicConfig(level=logging.INFO)

DB_URI = os.environ.get('DB_URI') or 'sqlite:///db.sqlite'
LOG_PATH = os.environ.get('LOG_PATH') or './data/access-logs.json'


def provision(uri, log_path):
    """
    Provision database for pipeline. Uses SQLAlchemy to make the provisioning
    db agnostic.
    """
    try:
        engine = sa.create_engine(uri)
        meta = sa.MetaData(bind=engine)
        logging.info('Connected to {}'.format(uri))

        logs = sa.Table('logs', meta,
                        Column('id', Integer, primary_key=True),
                        Column('ds', sa.Date),  # time
                        Column('ClientHost', String(15)),
                        Column('ClientPort', String(5)),
                        Column('ClientUsername', String(25)),
                        Column('DownstreamContentSize', Integer),
                        Column('DownstreamStatus', Integer),
                        Column('Duration', Integer),
                        Column('OriginContentSize', Integer),
                        Column('OriginDuration', Integer),
                        Column('OriginStatus', Integer),
                        Column('Overhead', Integer),
                        Column('RequestAddr', String(50)),
                        Column('RequestContentSize', Integer),
                        Column('RequestCount', Integer),
                        Column('RequestHost', String(50)),
                        Column('RequestMethod', String(10)),
                        Column('RequestPath', Text),
                        Column('RequestPort', String(10)),
                        Column('RequestProtocol', String(15)),
                        Column('RetryAttempts', Integer),
                        Column('RouterName', String(50)),
                        Column('ServiceAddr', String(25)),
                        Column('ServiceName', String(50)),
                        Column('ServiceScheme', String(10)),
                        Column('ServiceUrlPath', Text),
                        Column('ServiceUrlRawPath', Text),
                        Column('ServiceUrlRawQuery', Text),
                        Column('ServiceUrlFragment', Text),
                        Column('entryPointName', String(10)),
                        Column('level', String(10)),
                        Column('msg', Text))

        # this table stores metadata about the pipeline project
        meta_table = sa.Table('pipeline_meta', meta,
                              Column('id', Integer, primary_key=True),
                              Column('bytes_read', Integer, nullable=False),
                              Column('log_file', Text, nullable=False))

        meta.create_all()

        with engine.connect() as conn:
            q = """
            INSERT INTO pipeline_meta (bytes_read, log_file) 
            VALUES (%d, '%s')
            """ % (0, log_path)
            conn.execute(q)

        logging.info('Database provisioned!')
    except Exception:
        logging.exception('Error provisioning database {}'.format(uri))


if __name__ == "__main__":
    provision(DB_URI, LOG_PATH)
