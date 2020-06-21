#!/usr/bin/env python3
#
# Batch data pipeline to ingest web server logs into data warehouse table from
# log file. Persists position in the log file.
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


def transform_log_dict(log):
    """Transform and clean log dict"""
    log['ds'] = log['time']

    # service url transformation
    serviceUrl = log.get('ServiceURL')
    if serviceUrl:
        log['ServiceScheme'] = serviceUrl['ServiceScheme']
        log['ServiceUrlPath'] = serviceUrl['Path']
        log['ServiceUrlRawPath'] = serviceUrl['RawPath']
        log['ServiceUrlRawQuery'] = serviceUrl['RawQuery']
        log['ServiceUrlFragment'] = serviceUrl['Fragment']
        del log['ServiceURL']

    # remove uneeded entries
    del log['ClientAddr']
    del log['StartLocal']
    del log['StartUTC']
    del log['time']

    return log


def load_batch(uri, pos, log_file):
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
            with open(log_file, 'r') as f:
                f.seek(pos)

                # PROCESS LOGS
                pbar = tqdm(position=pos, total=file_size - pos)
                while bytes_read < file_size:
                    line = f.readline()

                    # check blank / empty lines
                    if len(line) != 0 and line.strip() != '':
                        log = transform_log_dict(json.loads(line))

                        # format dict into templated query for SQLAlchemy
                        # would be better to format this outside of the for loop.
                        q = 'INSERT INTO logs ({}) VALUES ({});'.format(
                            ', '.join(log.keys()),
                            ', '.join([':' + k for k in log.keys()]))
                        conn.execute(q, **log)

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
    logging.info('Running pipeline with DB: {}'.format(DB_URI))

    # RUN PIPELINE
    pos, log_file = setup(DB_URI)
    load_batch(DB_URI, pos, log_file)
