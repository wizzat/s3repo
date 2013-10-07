from s3cache import S3Repo
from testcase import DBTestCase
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.util import *
import unittest, psycopg2, json, os, s3cache

class LifecycleTest(DBTestCase):
    def test_creates(self):
        repo = S3Repo()
        repo.create_repository()

    def test_errors_if_already_exists(self):
        repo = S3Repo()
        repo.create_repository()
        with self.assertRaises(s3cache.RepoAlreadyExistsError):
            repo.create_repository()

    def test_does_not_error_if_does_not_exist(self):
        repo = S3Repo()
        repo.create_repository()
        repo.destroy_repository()

        self.assertFalse(table_exists(self.conn, "s3_objects"))

        repo.destroy_repository()

    def test_destroys_if_exists(self):
        repo = S3Repo()
        repo.create_repository()
        repo.destroy_repository()

        self.assertFalse(table_exists(self.conn, "s3_objects"))

    @skip_unfinished
    def test_expired_records_are_kept_until_repo_backup(self):
        pass

    @skip_unfinished
    def test_never_published_files_get_flushed(self):
        pass

    @skip_unfinished
    def test_cache_purge_using_atime(self):
        pass

    @skip_unfinished
    def test_restore_from_backup(self):
        pass

    @skip_unfinished
    def test_save_backup(self):
        pass
