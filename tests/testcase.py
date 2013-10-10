import unittest, psycopg2, json, os, s3repo, shutil
from s3repo import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.util import *
from pyutil.dateutil import *

class DBTestCase(AssertSQLMixin, unittest.TestCase):
    config = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
    db_info = config['database']

    conn = None
    def setUp(self):
        super(DBTestCase, self).setUp()
        set_now(now())
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
