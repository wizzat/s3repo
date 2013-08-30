import boto
import os
import json
import shutil
import psycopg2
import md5
from pyutil.pghelper import table_exists, execute, sql_where_from_params
from dbtable import DBTable

class RepoAlreadyExistsError(Exception): pass
class RepoFileNotUploadedError(Exception): pass

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
        if table_exists(self.conn, "s3_objects"):
            raise RepoAlreadyExistsError()

        self.conn.execute("CREATE SEQUENCE s3_obj_seq")
        self.conn.execute("""
            CREATE TABLE s3_objects (
                file_no          INTEGER default nextval('s3_obj_seq'),
                s3_bucket        VARCHAR(64),
                s3_key           VARCHAR(1024),
                origin           VARCHAR(256),
                md5              VARCHAR(16),
                file_size        INTEGER,
                date_created     TIMESTAMP DEFAULT now(),
                date_uploaded    TIMESTAMP,
                date_published   TIMESTAMP,
                date_expired     TIMESTAMP,
                attributes       HSTORE,
                published        BOOLEAN DEFAULT FALSE
        """)

        self.conn.execute("CREATE UNIQUE INDEX unq_s3_bucket_key ON s3_objects (s3_bucket, s3_key)")

    def destroy_repository(self):
        self.conn.execute("drop table if exists s3_objects");
        self.conn.commit()
        try:
            self.conn.execute("drop sequence s3_obj_seq");
            self.conn.commit()
        except psycopg2.ProgrammingError:
            self.conn.rollback()

    def create_file_from_local(self, local_path, s3_bucket = None, s3_key = None):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        remote_key = remote_key or str(uuid.uuid4())

        rf = self.RepoFile(s3_bucket, s3_key)

class _RepoFile(DBTable):
    table_name = 'repo_files'
    key_field  = 'file_no'
    fields = (
        'file_no',
        's3_bucket',
        's3_key',
        'published',
        'origin',
        'md5',
        'file_size',
        'date_created',
        'date_uploaded',
        'date_published',
        'date_expired',
        'attributes',
    )

    def local_path(self):
        return os.path.join(
            self.local_root,
            self.s3_bucket,
            self.s3_key,
        )

    def publish(self):
        if self.expired or not self.published:
            self.published = True
            self.date_expired = None
            self.date_published = now()
        self.upload()

    def expire(self):
        self.published = False
        if not self.date_expired:
            self.date_expired = now()

    def upload(self):
        if os.path.exists(self.local_path()) and not self.date_uploaded:
            self.date_uploaded = now()
            self.set_file_stats()

    def set_file_stats(self):
        self.file_size = os.stat(self.local_path()).st_size

        with open(self.local_path(), 'r') as fp:
            data = fp.read()
            self.md5 = md5.md5(data).hexdigest()

    def download(self):
        """
        Download the file to the local cache
        """
        if not self.date_uploaded:
            raise RepoFileNotUploadedError()

    def open(self):
        """
        Returns a file pointer to the current file.
        """
        pass
