import os, shutil, md5, uuid, socket, subprocess, gzip
import json
import psycopg2
import boto
from boto.s3.key import Key
from pyutil.pghelper import *
from pyutil.util import *
from pyutil.dateutil import *

__all__ = [
    'RepoError',
    'RepoAlreadyExistsError',
    'RepoFileNotUploadedError',
    'RepoFileAlreadyExistsError',
    'PurgingPublishedRecordError',
    'S3Repo',
]

class RepoError(Exception): pass
class RepoAlreadyExistsError(RepoError): pass
class RepoFileNotUploadedError(RepoError): pass
class RepoFileAlreadyExistsError(RepoError): pass
class RepoFileDoesNotExistLocallyError(RepoError): pass
class PurgingPublishedRecordError(RepoError): pass

class S3Repo(object):
    def __init__(self):
        self.s3_buckets = {}
        self.config     = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
        self.db_conn    = psycopg2.connect(**self.config['database'])
        self.s3_conn    = boto.connect_s3(
            self.config['s3_access_key'],
            self.config['s3_secret_key'],
        )

        class RepoFile(_RepoFile):
            local_root = self.config['local_root']
            conn       = self.db_conn
            s3_conn    = self.s3_conn
            s3_repo    = self

        self.RepoFile = RepoFile

    def create_repository(self):
        if table_exists(self.db_conn, "s3_objects"):
            raise RepoAlreadyExistsError()

        # Create the s3 bucket
        if not os.environ.get('OFFLINE', False):
            pass

        execute(self.db_conn, "CREATE SEQUENCE s3_obj_seq")
        execute(self.db_conn, """
            CREATE TABLE s3_objects (
                file_no          INTEGER default nextval('s3_obj_seq'),
                s3_bucket        VARCHAR(64),
                s3_key           VARCHAR(1024),
                origin           VARCHAR(256),
                md5              VARCHAR(32),
                file_size        INTEGER,
                date_created     TIMESTAMP DEFAULT now(),
                date_uploaded    TIMESTAMP,
                date_published   TIMESTAMP,
                date_backed_up   TIMESTAMP,
                date_expired     TIMESTAMP,
                attributes       HSTORE,
                published        BOOLEAN DEFAULT FALSE
            )
        """)

        execute(self.db_conn, "CREATE UNIQUE INDEX unq_s3_bucket_key ON s3_objects (s3_bucket, s3_key)")
        self.db_conn.commit()
        return self

    def destroy_repository(self):
        execute(self.db_conn, "DROP TABLE IF EXISTS s3_objects")
        self.db_conn.commit()

        try:
            execute(self.db_conn, "DROP SEQUENCE s3_obj_seq")
            self.db_conn.commit()
        except psycopg2.ProgrammingError:
            self.db_conn.rollback()

    def add_local_file(self, local_path, s3_key = None, s3_bucket = None, move = True, **attributes):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or os.path.basename(local_path)
        rf = self.add_file(s3_key, s3_bucket, **attributes)

        mkdirp(os.path.dirname(rf.local_path()))
        if move:
            shutil.move(local_path, rf.local_path())
        else:
            shutil.copy(local_path, rf.local_path())

        return rf

    def add_file(self, s3_key = None, s3_bucket = None, **attributes):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or str(uuid.uuid4())

        rf = list(self.RepoFile.find_by(s3_key = s3_key, s3_bucket = s3_bucket))
        if rf:
            raise RepoFileAlreadyExistsError(repr(rf))

        return self.RepoFile(
            s3_bucket    = s3_bucket,
            s3_key       = s3_key,
            origin       = socket.gethostname(),
            attributes   = attributes,
        ).update()

    def backup_db(self):
        """
        Creates a backup of the S3 Cache and uploads it to config['backup_s3_bucket']/s3cache_backups
        Backup name will be: "YYYY-MM-DD_HH:24:MI:SS.sql.gz"
        Ensures that no more than config['num_backups'] exist.
        """
        raise NotImplemented()

    def restore_db(self):
        """
        Queries config['backup_s3_bucket']/s3cache_backups and restores the latest backup.
        """
        raise NotImplemented()

    def cleanup_local_disk(self):
        """
        Recursively examines config['local_root'] and unlinks files which have been accessed more than config['local_atime_limit'] minutes ago.
        """
        raise NotImplemented()

    def commit(self):
        self.db_conn.commit()

class _RepoFile(DBTable):
    table_name = 's3_objects'
    key_field  = 'file_no'
    db_conn    = None
    s3_conn    = None
    fields     = (
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
        'date_backed_up',
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
        if self.date_expired or not self.published:
            self.published = True
            self.date_expired = None
            self.date_published = now()
        self.upload()
        self.update()

    def expire(self):
        self.published = False
        if not self.date_expired:
            self.date_expired = now()
        self.update()

    def upload(self):
        if self.date_uploaded:
            return

        if not os.path.exists(self.local_path()):
            raise RepoFileDoesNotExistLocallyError()

        self.set_file_stats()
        if not os.environ.get('OFFLINE', False):
            # Actually push to S3
            pass

        self.date_uploaded = now()

    def purge(self):
        if self.published:
            raise PurgingPublishedRecordError()

        assert len(self.delete()) ==  1

        if not os.environ.get('OFFLINE', False):
            # Actually delete from S3
            pass

    def set_file_stats(self):
        self.file_size = os.stat(self.local_path()).st_size
        self.md5 = subprocess.check_output([ "md5", "-q", self.local_path() ])[:-1]

    def download(self):
        """
        Download the file to the local cache
        """
        if not self.date_uploaded:
            raise RepoFileNotUploadedError()
        if os.path.exists(self.local_path()):
            return

    def open(self):
        """
        Returns a file pointer to the current file.
        """
        self.download()
        if self.s3_key.endswith(".gz"):
            return gzip.open(self.local_path(), 'r')
        else:
            return open(self.local_path(), 'r')

    def touch(self):
        """
        Ensures the repo file exists.
        """
        mkdirp(os.path.dirname(self.local_path()))
        with open(self.local_path(), 'a') as fp:
            fp.flush()

    def __repr__(self):
        return "RepoFile(s3://{s3_bucket}/{s3_key} ( {origin}:{local_path} )".format(
            s3_bucket = self.s3_bucket,
            s3_key = self.s3_key,
            origin = self.origin,
            local_path = self.local_path(),
        )
