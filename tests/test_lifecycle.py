import unittest, psycopg2, json, os
import s3repo
from testcase import DBTestCase
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *

class LifecycleTest(DBTestCase):
    def test_drop_and_create(self):
        self.assertTrue(table_exists(self.conn, "s3_repo")) # Repo already exists from setup

        self.repo.destroy_repository()
        self.assertFalse(table_exists(self.conn, "s3_repo"))

        self.repo.destroy_repository() # Don't throw exceptions for an uncreated repo
        self.assertFalse(table_exists(self.conn, "s3_repo"))

        self.repo.create_repository() # Recreate repo without error
        self.assertTrue(table_exists(self.conn, "s3_repo"))

    def test_errors_if_already_exists(self):
        with self.assertRaises(s3repo.RepoAlreadyExistsError):
            self.repo.create_repository()

    def test_never_published_files_get_flushed(self):
        rf1 = self.repo.add_file(s3_key = "abc")
        rf2 = self.repo.add_file(s3_key = "def")
        rf3 = self.repo.add_file(s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.touch()
            rf.date_created = now() - weeks(2)
            rf.update()
        rf1.publish()
        rf2.publish()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_key',  'date_published',  'published',  ],
            [ rf1.file_no,  'abc',     now(),             True,         ],
            [ rf2.file_no,  'def',     now(),             True,         ],
            [ rf3.file_no,  'ghi',     None,              False,        ],
        )

        self.repo.cleanup_unpublished_files()
        self.assertTrue(os.path.exists(rf1.local_path()))
        self.assertTrue(os.path.exists(rf2.local_path()))
        self.assertFalse(os.path.exists(rf3.local_path()))
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_key',  'date_published',  'published',  ],
            [ rf1.file_no,  'abc',     now(),             True,         ],
            [ rf2.file_no,  'def',     now(),             True,         ],
        )

    def test_unpublished_files_are_only_removed_for_locally_created_content(self):
        rf1 = self.repo.add_file(s3_key = "abc")
        rf2 = self.repo.add_file(s3_key = "def")
        rf3 = self.repo.add_file(s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.touch()
            rf.date_created = now() - weeks(2)
            rf.update()

        rf3.origin = 'abc'
        rf3.update()
        self.repo.commit()

        self.repo.cleanup_unpublished_files()

        self.assertFalse(os.path.exists(rf1.local_path()))
        self.assertFalse(os.path.exists(rf2.local_path()))
        self.assertTrue(os.path.exists(rf3.local_path()))
        self.repo.commit()

    def test_timelimit_for_deleting_unpublished_files(self):
        rf1 = self.repo.add_file(s3_key = "abc")
        rf2 = self.repo.add_file(s3_key = "def")
        rf3 = self.repo.add_file(s3_key = "ghi")

        dt = now() - weeks(1) - seconds(2)
        for i, rf in enumerate([ rf1, rf2, rf3 ]):
            rf.touch()
            rf.date_created = dt + seconds(i)
            rf.update()

        self.repo.cleanup_unpublished_files()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    ],
            [ rf3.file_no,  ],
        )

    def test_save_backup(self):
        rf1 = self.repo.add_file(s3_key = "abc")
        rf2 = self.repo.add_file(s3_key = "bcd")
        rf3 = self.repo.add_file(s3_key = "cde")
        rf4 = self.repo.backup_db()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               rf4.file_size,  ],
        )

    @skip_unfinished
    def test_cache_purge_using_atime(self):
        pass

class LifecycleRemoteTest(DBTestCase):
    requires_online = True

    @skip_offline
    def test_restore_from_backup(self):
        import time
        rf1 = self.repo.add_file(s3_key = "abc")
        rf2 = self.repo.add_file(s3_key = "bcd")
        rf3 = self.repo.add_file(s3_key = "cde")
        rf4 = self.repo.backup_db()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               rf4.file_size,  ],
        )
        self.conn.commit()

        self.repo.destroy_repository()
        self.repo.commit()
        self.repo = s3repo.S3Repo()
        self.repo.restore_db()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               -1,             ],
        )

