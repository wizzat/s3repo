from s3repo import S3Repo
from testcase import DBTestCase
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
import unittest, psycopg2, json, os, s3repo

class LifecycleTest(DBTestCase):
    def test_creates(self):
        repo = S3Repo()
        repo.create_repository()

    def test_errors_if_already_exists(self):
        repo = S3Repo()
        repo.create_repository()
        with self.assertRaises(s3repo.RepoAlreadyExistsError):
            repo.create_repository()

    def test_does_not_error_if_does_not_exist(self):
        repo = S3Repo()
        repo.create_repository()
        repo.destroy_repository()

        self.assertFalse(table_exists(self.conn, "s3_repo"))

        repo.destroy_repository()

    def test_destroys_if_exists(self):
        repo = S3Repo()
        repo.create_repository()
        repo.destroy_repository()

        self.assertFalse(table_exists(self.conn, "s3_repo"))

    @skip_unfinished
    def test_expired_records_are_kept_until_repo_backup(self):
        pass

    @skip_unfinished
    def test_timelimit_for_deleting_unpublished_files(self):
        pass

    def test_never_published_files_get_flushed(self):
        repo = S3Repo()
        repo.create_repository()
        rf1 = repo.add_file(s3_key = "abc")
        rf2 = repo.add_file(s3_key = "def")
        rf3 = repo.add_file(s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.touch()
            rf.date_created = now() - weeks(2)
            rf.update()
        rf1.publish()
        rf2.publish()
        repo.commit()

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

        repo.cleanup_unpublished_files()
        self.assertTrue(os.path.exists(rf1.local_path()))
        self.assertTrue(os.path.exists(rf2.local_path()))
        self.assertFalse(os.path.exists(rf3.local_path()))
        repo.commit()

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
        repo = S3Repo()
        repo.create_repository()

        rf1 = repo.add_file(s3_key = "abc")
        rf2 = repo.add_file(s3_key = "def")
        rf3 = repo.add_file(s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.touch()
            rf.date_created = now() - weeks(2)
            rf.update()

        rf3.origin = 'abc'
        rf3.update()
        repo.commit()

        repo.cleanup_unpublished_files()

        self.assertFalse(os.path.exists(rf1.local_path()))
        self.assertFalse(os.path.exists(rf2.local_path()))
        self.assertTrue(os.path.exists(rf3.local_path()))
        repo.commit()

    @skip_unfinished
    def test_cache_purge_using_atime(self):
        pass

    @skip_unfinished
    def test_restore_from_backup(self):
        pass

    @skip_unfinished
    def test_save_backup(self):
        pass
