#!/usr/bin/env python3
#
# Batch data pipeline to ingest web server logs into staging table directly
# from log file. Raw logs are ingested with a datestamp so other pipelines can
# run jobs by date.
#
# This pipeline is designed to be run quite frequentely to avoid large batch
# sizes being ingested.
#
# Written by Nicholas Cannon
import sqlalchemy as sa
import logging
import os
import json
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

DB_URI = os.environ.get('DB_URI') or 'sqlite:///db.sqlite'


def setup(uri):
    """Setup the pipeline by grabbing meta data from pipeline db"""
    try:
        engine = sa.create_engine(uri)
        with engine.connect() as conn:
            q = """
            SELECT COUNT(*), bytes_read, log_file FROM pipeline_meta;
            """
            r = conn.execute(q)

            for row in r:
                if row[0] > 1:
                    raise Exception(
                        'More than one entry in pipeline meta table')
                else:
                    return row[1], row[2]
    except Exception:
        logging.exception('Setup Error!')


def load_raw_batch(uri, pos, log_file):
    """
    Open log file and ingest raw batch into staging table. Doesn't ingest by
    datestamp but instead ingests all new data from logfile.
    """
    # SETUP
    try:
        # check valid log file
        if not os.path.exists(log_file):
            raise Exception('Logfile does not exist {}'.format(log_file))

        # check if there are any logs to ingest
        file_size = os.path.getsize(log_file)
        if (file_size + 1) == pos:
            logging.info(
                'No new logs to ingest! Log size: {} pos: {}'.format(file_size, pos))
            return

        # validate log position (error if more than 1 byte larger)
        if file_size < pos:
            raise Exception('Log position is greater than log file size')

        # test db connection
        engine = sa.create_engine(uri)
        engine.connect().close()
    except Exception as e:
        logging.exception('Process batch setup error!')

    # PROCESS BATCH
    try:
        logging.info('Log size: {} file pos: {}'.format(file_size, pos))

        bytes_read = pos
        with engine.connect() as conn:
            q = 'INSERT INTO logs_staged (ds, log) VALUES (:ds, :log);'
            with open(log_file, 'r') as f:
                f.seek(pos)

                # PROCESS LINES
                pbar = tqdm(position=pos, total=file_size - pos)
                while bytes_read < file_size:
                    line = f.readline()

                    # check blank / empty lines
                    if len(line) != 0 and line.strip() != '':
                        ds = json.loads(line).get('time')
                        conn.execute(q, {'ds': ds, 'log': line})

                    line_bytes = len(line.encode())
                    pbar.update(line_bytes)
                    bytes_read += line_bytes
                pbar.close()

        logging.info('Ingested batch! {} of {} bytes ingested!'.format(
            bytes_read, file_size))
    except Exception:
        logging.exception('Process batch error!')
    finally:
        # update metadata table to where we ended up ingesting even if an error
        # occurred. This way we can re-process starting from the error.
        with engine.connect() as conn:
            q = 'UPDATE pipeline_meta SET bytes_read = :bytes WHERE id = 1;'
            conn.execute(q, {'bytes': bytes_read + 1})  # increment byte!!
        logging.info('Updated meta table')


if __name__ == "__main__":
    # RUN PIPELINE
    pos, log_file = setup(DB_URI)
    load_raw_batch(DB_URI, pos, log_file)
