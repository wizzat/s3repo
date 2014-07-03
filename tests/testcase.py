import unittest, shutil, tempfile, os
import s3repo
import s3repo.common
import pyutil.pgtestutil
from pyutil.pghelper import *
from pyutil.util import *
from pyutil.dateutil import *
from pyutil.decorators import MemoizeResults

class DBTestCase(pyutil.pgtestutil.PgTestCase):
    setup_database = True
    requires_online = False
    config = s3repo.common.load_cfg()
    s3_conn = s3repo.common.s3_conn()
    random_files = []

    def conn(self, name = 'testconn'):
        conn = s3repo.common.db_conn(name)
        conn.autocommit = True

        return conn

    def setUp(self):
        super(DBTestCase, self).setUp()

        execute(self.conn(), 'TRUNCATE TABLE s3_repo.hosts, s3_repo.tags, s3_repo.files, s3_repo.file_tags, s3_repo.path_tags, s3_repo.downloads')

    def teardown_connections(self):
        s3repo.common.db_mgr.rollback()
        MemoizeResults.clear()

        for cache_obj in DBTable.__subclasses__():
            cache_obj.clear_cache()

        for filename in self.random_files:
            swallow((OSError, IOError), lambda: os.unlink(filename))

        if is_online():
            # We need to clear out all the S3 objects
            for bucket in { self.config['s3.default_bucket'], self.config['s3.backup_bucket'] }:
                for key in self.s3_conn.get_bucket(bucket).list():
                    key.delete()

    def s3_list_bucket(self, name, prefix=''):
        """
        Uses the generic S3 connection to list the contents of a bucket
        """
        return [ x.name for x in self.s3_conn.get_bucket(name).list(prefix) ]

    def s3_put_string(self, bucket, key, value):
        bucket = self.s3_conn.get_bucket(bucket)
        key = Key(bucket, key)
        key.set_contents(value)

    def s3_get_string(self, bucket, key):
        bucket = self.s3_conn.get_bucket(bucket)
        key = Key(bucket, key)
        key.get_contents()

    def random_filename(self, contents = '', **kwargs):
        mkdirp('/tmp/s3repo')
        fp = tempfile.NamedTemporaryFile(
            dir    = '/tmp/s3repo',
            delete = False,
            **kwargs
        )

        fp.write(contents)
        self.random_files.append(fp.name)

        return fp.name
