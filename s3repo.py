import os, shutil, md5, uuid, socket, subprocess, gzip
import json
import psycopg2
import boto
from boto.s3.key import Key, compute_md5
from pyutil.pghelper import *
from pyutil.util import *
from pyutil.dateutil import *
from pyutil.decorators import *

__all__ = [
    'RepoError',
    'RepoAlreadyExistsError',
    'RepoFileNotUploadedError',
    'RepoFileAlreadyExistsError',
    'RepoDownloadError',
    'RepoUploadError',
    'PurgingPublishedRecordError',
    'S3Repo',
]

class RepoError(Exception): pass
class RepoAlreadyExistsError(RepoError): pass
class RepoNoBackupsError(RepoError): pass
class RepoFileNotUploadedError(RepoError): pass
class RepoFileAlreadyExistsError(RepoError): pass
class RepoFileDoesNotExistLocallyError(RepoError): pass
class RepoUploadError(RepoError): pass
class RepoDownloadError(RepoError): pass
class PurgingPublishedRecordError(RepoError): pass

class S3Repo(object):
    def __init__(self):
        self.s3_buckets = {}
        self.config     = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
        self.db_mgr     = ConnMgr(**self.config['database'])
        self.db_conn    = self.db_mgr.getconn("conn")
        self.s3_conn    = boto.connect_s3(
            self.config['s3_access_key'],
            self.config['s3_secret_key'],
        )

        class RepoFile(_RepoFile):
            local_root = self.config['local_root']
            conn       = self.db_conn
            s3_conn    = self.s3_conn

        self.RepoFile = RepoFile
        self.commit = self.db_conn.commit
        self.rollback = self.db_conn.rollback
        self.find_by = self.RepoFile.find_by

    def create_repository(self):
        if table_exists(self.db_conn, "s3_repo"):
            raise RepoAlreadyExistsError()

        execute(self.db_conn, "CREATE SEQUENCE s3_repo_seq")
        execute(self.db_conn, """
            CREATE TABLE s3_repo (
                file_no          INTEGER default nextval('s3_repo_seq'),
                s3_bucket        VARCHAR(64),
                s3_key           VARCHAR(1024),
                file_type        VARCHAR(64),
                period           TIMESTAMP,
                origin           TEXT,
                md5              TEXT,
                b64              TEXT,
                file_size        INTEGER,
                date_created     TIMESTAMP DEFAULT now(),
                date_uploaded    TIMESTAMP,
                date_published   TIMESTAMP,
                date_archived    TIMESTAMP,
                date_expired     TIMESTAMP,
                published        BOOLEAN DEFAULT FALSE
            )
        """)

        execute(self.db_conn, "CREATE UNIQUE INDEX unq_s3_bucket_key ON s3_repo (s3_bucket, s3_key)")
        self.db_conn.commit()
        return self

    def destroy_repository(self):
        execute(self.db_conn, "DROP TABLE IF EXISTS s3_repo")
        self.db_conn.commit()

        try:
            execute(self.db_conn, "DROP SEQUENCE s3_repo_seq")
            self.db_conn.commit()
        except psycopg2.ProgrammingError:
            self.db_conn.rollback()

    def add_local_file(self, local_path, s3_key = None, s3_bucket = None, move = True, **kwargs):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or os.path.basename(local_path)
        rf = self.add_file(s3_key, s3_bucket, **kwargs)

        mkdirp(os.path.dirname(rf.local_path()))
        if move:
            shutil.move(local_path, rf.local_path())
        else:
            shutil.copy(local_path, rf.local_path())

        return rf

    def add_file(self, s3_key = None, s3_bucket = None, **kwargs):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or str(uuid.uuid4())

        rf = list(self.RepoFile.find_by(s3_key = s3_key, s3_bucket = s3_bucket))
        if rf:
            raise RepoFileAlreadyExistsError(repr(rf))

        return self.RepoFile(
            s3_bucket = s3_bucket,
            s3_key    = s3_key,
            origin    = socket.gethostname(),
            **kwargs
        ).update()

    def backup_db(self):
        """
        Creates a backup of the S3 Cache and uploads it to config['backup_s3_bucket']/s3repo_backups
        Backup name will be: "YYYY-MM-DD_HH:24:MI:SS.sql.gz"
        Ensures that no more than config['num_backups'] exist.
        """
        backup_file = self.add_file(
            s3_bucket      = self.config['backup_s3_bucket'],
            s3_key         = now().strftime("s3repo_backups/%Y%m%d%H%M%S.sql.gz"),
            date_published = now(),
            published      = True,
        )

        with backup_file.open('w') as fp:
            self.db_conn.cursor().copy_to(fp, 's3_repo', columns = self.RepoFile.fields)

        backup_file.publish()

        return backup_file

    def restore_db(self):
        """
        Queries config['backup_s3_bucket']/s3repo_backups and restores the latest backup.
        """
        backup_bucket = self.s3_conn.get_bucket(self.config['backup_s3_bucket'])
        remote_backup_files = backup_bucket.list("s3repo_backups")
        self.create_repository()

        try:
            last_backup = sorted(remote_backup_files, key=lambda x: x.name)[-1]
        except IndexError:
            raise RepoNoBackupsError()

        local_path = os.path.join(
            self.config['local_root'],
            self.config['backup_s3_bucket'],
            last_backup.name,
        )

        last_backup.get_contents_to_filename(local_path)

        with gzip.open(local_path, 'r') as fp:
            self.db_conn.cursor().copy_from(fp, 's3_repo', columns = self.RepoFile.fields)

        rf = list(self.RepoFile.find_by(
            s3_bucket = self.config['backup_s3_bucket'],
            s3_key    = last_backup.name,
        ))[0]
        rf.file_size = -1
        rf.update()

        self.db_conn.commit()
        return last_backup

    def cleanup_unpublished_files(self):
        """
        Purges local files which have not been published
        """
        unpublished_files = self.find_by(origin = socket.gethostname(), published = False, date_published = None)

        for rf in unpublished_files:
            if rf.date_created < now() - weeks(1):
                rf.purge()

    def cleanup_local_disk(self):
        """
        Recursively examines config['local_root'] and unlinks files which have been accessed more than config['local_atime_limit'] minutes ago.
        """
        raise NotImplemented()

class _RepoFile(DBTable):
    table_name = 's3_repo'
    key_field  = 'file_no'
    db_conn    = None
    s3_conn    = None
    fields     = (
        'file_no',
        's3_bucket',
        's3_key',
        'file_type',
        'period',
        'published',
        'origin',
        'md5',
        'b64',
        'file_size',
        'date_created',
        'date_uploaded',
        'date_published',
        'date_archived',
        'date_expired',
    )

    def s3_path(self):
        return "s3://{}/{}".format(self.s3_bucket, self.s3_key)

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

        if not self.file_size:
            with open(self.local_path(), 'r') as fp:
                self.md5, self.b64, self.file_size = compute_md5(fp)

        if is_online():
            remote_bucket = self.s3_conn.get_bucket(self.s3_bucket)
            remote_key = Key(remote_bucket, self.s3_key)
            remote_key.set_contents_from_filename(self.local_path(), md5=(self.md5, self.b64, self.file_size))

        self.date_uploaded = now()

    def purge(self):
        if self.published:
            raise PurgingPublishedRecordError()

        assert len(self.delete()) ==  1

        if is_online():
            remote_bucket = self.s3_conn.get_bucket(self.s3_bucket)
            remote_key = Key(remote_bucket, self.s3_key)
            remote_bucket.delete_key(remote_key)

        swallow(OSError, lambda: os.unlink(self.local_path()))

    def download(self):
        """
        Download the file to the local cache
        """
        if not self.date_uploaded:
            raise RepoFileNotUploadedError()

        if os.path.exists(self.local_path()):
            return

        assert_online()

        remote_key = Key(self.s3_conn.get_bucket(self.s3_bucket), self.s3_key)
        remote_key.get_contents_to_filename(self.local_path())

        if self.md5:
            real_md5 = subprocess.check_output([ "md5", "-q", self.local_path() ])[:-1]
            if real_md5 != self.md5:
                raise RepoDownloadError()

    def open(self, mode='r'):
        """
        Returns a file pointer to the current file.
        """
        if mode == 'r' and self.date_uploaded:
            self.download()
        elif mode == 'w':
            mkdirp(os.path.dirname(self.local_path()))

        if self.s3_key.endswith(".gz"):
            return gzip.open(self.local_path(), mode)
        else:
            return open(self.local_path(), mode)

    def touch(self, contents = ""):
        """
        Ensures the repo file exists.
        """
        mkdirp(os.path.dirname(self.local_path()))
        with open(self.local_path(), 'a') as fp:
            if contents:
                fp.write(contents)
            fp.flush()

    def __repr__(self):
        return "RepoFile({s3_path} ( {origin}:{local_path} )".format(
            s3_path    = self.s3_path(),
            local_path = self.local_path(),
            origin     = self.origin,
        )
