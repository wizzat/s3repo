from s3cache import S3Repo
from pyutil.testutil import *
from pyutil.util import *
import unittest, psycopg2, json, os

class CreateTest(unittest.TestCase):
    conn = None
    def setUp(self):
        super(CreateTest, self).setUp()
        if not self.conn:
            config = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
            self.conn = psycopg2.connect(**config['database'])

    def test_creates(self):
        repo = S3Repo()
        repo.create_repository()

    def test_errors_if_already_exists(self):
        repo = S3Repo()
        repo.create_repository()
        with self.assertRaises(psycopg2.ProgrammingError):
            repo.create_repository()

if __name__ == '__main__':
    unittest.main()
