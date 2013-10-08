import unittest, psycopg2, json, os, s3cache, shutil
from s3cache import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.util import *
from pyutil.dateutil import *

class DBTestCase(unittest.TestCase, AssertSQLMixin):
    conn = None
    def setUp(self):
        super(DBTestCase, self).setUp()
        set_now(now())
        if not self.conn:
            config = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
            mkdirp(config['local_root'])
            shutil.rmtree(config['local_root'])
            self.conn = psycopg2.connect(**config['database'])

        execute(self.conn, "DROP TABLE IF EXISTS s3_objects")
        self.conn.commit()

        try:
            execute(self.conn, "DROP SEQUENCE s3_obj_seq")
            self.conn.commit()
        except psycopg2.ProgrammingError:
            self.conn.commit()

    def tearDown(self):
        super(DBTestCase, self).tearDown()
        if self.conn:
            self.conn.commit()
            self.conn.close()
