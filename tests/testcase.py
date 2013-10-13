import unittest, psycopg2, json, os, s3repo, shutil, boto
from s3repo import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.util import *
from pyutil.dateutil import *
from s3util import *

class DBTestCase(AssertSQLMixin, unittest.TestCase):
    requires_online = False
    config = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
    db_info = config['database']

    conn = None
    s3_conn = boto.connect_s3(
        config['s3_access_key'],
        config['s3_secret_key'],
    )

    def setUp(self):
        super(DBTestCase, self).setUp()
        set_now(now())
        if not self.requires_online:
            set_online(False)

        self.setup_connections()

        try:
            shutil.rmtree(self.config['local_root'])
        except (OSError, IOError):
            pass

        execute(self.conn, "DROP TABLE IF EXISTS s3_repo")
        self.conn.commit()

        try:
            execute(self.conn, "DROP SEQUENCE s3_repo_seq")
            self.conn.commit()
        except psycopg2.ProgrammingError:
            self.conn.commit()

    def tearDown(self):
        super(DBTestCase, self).tearDown()
        self.teardown_connections()

        if is_online():
            # We need to clear out all the S3 objects
            for bucket in { self.config['default_s3_bucket'], self.config['backup_s3_bucket'] }:
                for key in list_bucket(bucket):
                    key.delete()
        reset_online()
