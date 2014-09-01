import os, subprocess
import s3repo.common
import s3repo.exceptions
import s3repo.tag
import pyutil.pghelper
import pyutil.dbtable
from boto.s3.key import Key, compute_md5
from pyutil.decorators import memoize
from pyutil.dateutil import *
from pyutil.util import *
from pyutil.util import is_online, assert_online


class S3Bucket(pyutil.dbtable.DBTable):
    table_name = 's3_repo.s3_buckets'
    memoize    = True
    conn       = s3repo.common.db_conn()

    id_field = 's3_bucket_id'
    key_fields = [
        's3_bucket'
    ]

    fields = [
        's3_bucket_id',
        's3_bucket',
    ]

class LocalPath(pyutil.dbtable.DBTable):
    table_name = 's3_repo.paths'
    conn       = s3repo.common.db_conn()

    id_field   = 'path_id'
    key_fields = [
        'local_path'
    ]
    fields = [
        'path_id',
        'local_path',
    ]

    def find_current(self):
        results = RepoFile.find_by_sql("""
            select *
            from s3_repo.current_files
            where path_id = %(path_id)s
        """, path_id = self.path_id)

        if not results:
            return None
        else:
            return results[0]


class RepoFile(pyutil.dbtable.DBTable):
    table_name = 's3_repo.files'
    conn       = s3repo.common.db_conn()

    id_field   = 'file_id'
    key_fields = [
        's3_bucket_id',
        's3_key',
    ]

    fields = [
        'file_id',
        's3_bucket_id',
        's3_key',
        'path_id',
        'published',
        'origin',
        'md5',
        'b64',
        'guid',
        'file_size',
        'date_created',
        'date_uploaded',
        'date_published',
        'date_archived',
        'date_expired',
    ]

    def s3_path(self):
        return "s3://{}/{}".format(self.s3_bucket(), self.s3_key)

    @memoize(obj=True)
    def s3_bucket(self):
        return S3Bucket.find_by_id(self.s3_bucket_id).s3_bucket

    @memoize(obj=True)
    def local_path(self):
        return LocalPath.find_by_id(self.path_id).local_path

    def after_insert(self):
        super(RepoFile, self).after_insert()
        s3repo.host.RepoFileDownload.flag_download(self)

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
            raise s3repo.exceptions.RepoFileDoesNotExistLocallyError()

        if not self.file_size:
            with open(self.local_path(), 'r') as fp:
                self.md5, self.b64, self.file_size = compute_md5(fp)

        if is_online():
            remote_bucket = s3repo.common.s3_conn().get_bucket(self.s3_bucket())
            remote_key = Key(remote_bucket, self.s3_key)
            remote_key.set_contents_from_filename(self.local_path(), md5=(self.md5, self.b64, self.file_size))

        self.date_uploaded = now()

    def purge(self):
        if self.published:
            raise s3repo.exceptions.PurgingPublishedRecordError()

        if is_online():
            remote_bucket = s3repo.common.s3_conn().get_bucket(self.s3_bucket())
            remote_key = Key(remote_bucket, self.s3_key)
            remote_bucket.delete_key(remote_key)

        self.unlink()

    def download(self):
        """
        Download the file to the local cache
        """
        if not self.date_uploaded:
            raise s3repo.exceptions.RepoFileNotUploadedError()

        if os.path.exists(self.local_path()):
            return

        assert_online()

        remote_key = Key(s3repo.common.s3_conn().get_bucket(self.s3_bucket()), self.s3_key)
        remote_key.get_contents_to_filename(self.local_path())

        if self.md5:
            real_md5 = subprocess.check_output([ "md5", "-q", self.local_path() ])[:-1]
            if real_md5 != self.md5:
                raise s3repo.exceptions.RepoDownloadError()

        s3repo.host.RepoFileDownload.flag_download(self)

    def unlink(self):
        """
        Remove the file from the local cache
        """
        s3repo.host.RepoFileDownload.remove_download(self)
        if os.path.exists(self.local_path()):
            os.unlink(self.local_path())

    def open(self, mode='r'):
        """
        Returns a file pointer to the current file.
        """
        if mode == 'r' and self.date_uploaded:
            self.download()
        elif mode == 'w':
            mkdirp(os.path.dirname(self.local_path()))

        s3repo.host.RepoFileDownload.update_access_time(self)

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

    def tag_path(self, *tag_names):
        s3repo.tag.RepoPathTag.tag_path(self.path_id, *tag_names)

    def untag_path(self, *tag_names):
        s3repo.tag.RepoPathTag.untag_path(self.path_id, *tag_names)

    def tag_file(self, *tag_names):
        s3repo.tag.RepoFileTag.tag_file(self.file_id, *tag_names)

    def untag_file(self, *tag_names):
        s3repo.tag.RepoFileTag.untag_file(self.file_id, *tag_names)

    date_tags = {
        'hour'  : lambda x: 'hour='  + format_hour(x),
        'day'   : lambda x: 'day='   + format_day(x),
        'week'  : lambda x: 'week='  + format_week(x),
        'month' : lambda x: 'month=' + format_month(x),
    }

    tag_funcs = {
        'hour'  : [ 'hour',   'day',   'week',     'month' ],
        'day'   : [ 'day',    'week',  'month' ],
        'week'  : [ 'week',   ],
        'month' : [ 'month',  ],
    }

    def tag_date(self, period, type='hour'):
        period = coerce_date(period)
        self.tag_path(*[ self.date_tags[period_type](period) for period_type in self.tag_funcs[type] ])

    def __repr__(self):
        return "RepoFile({s3_path} ( {origin}:{local_path} )".format(
            s3_path    = self.s3_path(),
            local_path = self.local_path(),
            origin     = self.origin,
        )

