import unittest, psycopg2, json, os
import s3repo.host
import s3repo.common
from testcase import DBTestCase
from s3repo import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *

class S3RepoTest(DBTestCase):
    def test_never_published_files_get_flushed(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.date_created = now() - weeks(2)
            rf.update()

        rf1.publish()
        rf2.publish()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 'file_id',    's3_key',  'date_published',  'published',  ],
            [ rf1.file_id,  'abc',     now(),             True,         ],
            [ rf2.file_id,  'def',     now(),             True,         ],
            [ rf3.file_id,  'ghi',     None,              False,        ],
        )

        S3Repo.cleanup_unpublished_files()
        self.assertTrue(os.path.exists(rf1.local_path()))
        self.assertTrue(os.path.exists(rf2.local_path()))
        self.assertFalse(os.path.exists(rf3.local_path()))
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 'file_id',    's3_key',  'date_published',  'published',  ],
            [ rf1.file_id,  'abc',     now(),             True,         ],
            [ rf2.file_id,  'def',     now(),             True,         ],
        )

    def test_unpublished_files_are_only_removed_for_locally_created_content(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.date_created = now() - weeks(2)
            rf.update()

        rf3.origin = s3repo.host.RepoHost.find_or_create('abc').host_id
        rf3.update()
        S3Repo.commit()

        S3Repo.cleanup_unpublished_files()

        self.assertFalse(os.path.exists(rf1.local_path()))
        self.assertFalse(os.path.exists(rf2.local_path()))
        self.assertTrue(os.path.exists(rf3.local_path()))
        S3Repo.commit()

    def test_timelimit_for_deleting_unpublished_files(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        dt = now() - hours(self.config['unpublished.cleanup.hours']) - seconds(2)
        for i, rf in enumerate([ rf1, rf2, rf3 ]):
            rf.date_created = dt + seconds(i)
            rf.update()

        S3Repo.cleanup_unpublished_files()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 'file_id',    ],
            [ rf3.file_id,  ],
        )

    @skip_unfinished
    def test_cache_purge_using_atime(self):
        pass

