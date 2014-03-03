import unittest, psycopg2, json, os, s3repo, shutil, boto, os.path
from s3repo import S3Repo, raw_cfg
from pyutil.pghelper import *
from pyutil.pgtestutil import *
from pyutil.util import *
from pyutil.dateutil import *

class DBTestCase(PgTestCase):
    setup_database = True
    requires_online = False
    config = s3repo.raw_cfg()
    db_info = config['database']

    conn = None
    s3_conn = boto.connect_s3(
        config['s3_access_key'],
        config['s3_secret_key'],
    )

    def setUp(self):
        super(DBTestCase, self).setUp()
        if not self.requires_online:
            set_online(False)

        try:
            shutil.rmtree(self.config['local_root'])
        except (OSError, IOError), e:
            pass

        self.repo = S3Repo()
        self.repo.create_repository(False)
        self.repo.commit()

        execute(self.conn, 'truncate table s3_repo, s3_tags, s3_repo_tags')
        self.conn.commit()

    def tearDown(self):
        super(DBTestCase, self).tearDown()

        if is_online():
            # We need to clear out all the S3 objects
            for bucket in { self.config['default_s3_bucket'], self.config['backup_s3_bucket'] }:
                for key in self.list_bucket(bucket):
                    key.delete()
        reset_online()

    def list_bucket(self, name, prefix=''):
        """
        Uses the generic S3 connection to list the contents of a bucket
        """
        return self.s3_conn.get_bucket(name).list(prefix)

    def put_string(self, bucket, key, value):
        bucket = self.s3_conn.get_bucket(bucket)
        key = Key(bucket, key)
        key.set_contents(value)

