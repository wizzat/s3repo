import boto
import os
import json
import shutil
import psycopg2
from pyutil.pghelper import table_exists, execute, sql_where_from_params

class S3Repo(object):
    def __init__(self):
        self.config = json.load(os.environ['S3CACHE_CONFIG'])
        self.conn   = psycopg2.connect(**config['database'])

        self.setup_repo_file()

    def setup_repo_file(self):
        class RepoFile(_RepoFile):
            conn = self.conn
        self.RepoFile = RepoFile

    def create_repository(self):
        self.conn.execute("CREATE SEQUENCE s3_obj_seq")
        self.conn.execute("""
            CREATE TABLE s3_objects (
                file_no          INTEGER default nextval('s3_obj_seq'),
                s3_bucket        VARCHAR(64),
                s3_key           VARCHAR(1024),
                file_type        VARCHAR(64),
                file_date        TIMESTAMP,
                origin           VARCHAR(256),
                md5              VARCHAR(16),
                num_rows         INTEGER,
                file_size        INTEGER,
                date_created     TIMESTAMP DEFAULT now(),
                date_uploaded    TIMESTAMP,
                date_published   TIMESTAMP,
                date_unpublished TIMESTAMP,
                published        BOOLEAN DEFAULT FALSE
        """)

        self.conn.execute("CREATE UNIQUE INDEX unq_s3_bucket_key ON s3_objects (s3_bucket, s3_key)")

    def destroy_repository(self):
        pass

    def create_file_from_local(self, local_path):
        pass

    def flag_published(self, repo_file):
        pass

    def s3_sync_published(self):
        pass

    def flush_unpublished(self, age=28800):
        pass

class _RepoFile(DBTable):
    table_name = 'repo_files'
    key_field  = 'file_no'
    fields = (
        'file_no',
        's3_bucket',
        's3_key',
        'file_type',
        'file_date',
        'origin',
        'md5',
        'num_rows',
        'file_size',
        'date_created',
        'date_uploaded',
        'date_published',
        'date_unpublished',
        'published',
    )
