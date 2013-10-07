import unittest, tempfile
from s3cache import *
from pyutil.testutil import *
from pyutil.dateutil import *
from testcase import *

class RemoteFileMgmtTest(DBTestCase):
    @skip_offline
    @skip_unfinished
    def test_publish_uploads_file(self):
        pass

    @skip_offline
    @skip_unfinished
    def test_upload_puts_files_on_s3(self):
        pass

    @skip_offline
    @skip_unfinished
    def test_upload_stores_md5(self):
        pass

    @skip_offline
    @skip_unfinished
    def test_purge_removes_from_s3(self):
        pass

    @skip_offline
    @skip_unfinished
    def test_expired_records_purged_from_s3(self):
        pass

class LocalFileMgmtTest(DBTestCase):
    def test_add_file_moves_file_into_local_cache(self):
        filename = None
        with tempfile.NamedTemporaryFile(delete = False) as fp:
            fp.write("herro!\n")
            fp.flush()

            self.assertTrue(os.path.exists(fp.name))

            repo = S3Repo()
            repo.create_repository()
            rf = repo.add_local_file(fp.name)
            repo.commit()

            self.assertFalse(os.path.exists(fp.name))
            self.assertTrue(os.path.exists(rf.local_path()))
            self.assertEqual(slurp(rf.local_path()), "herro!\n")

    def test_add_file_copies_file_into_local_cache(self):
        filename = None
        with tempfile.NamedTemporaryFile(delete = True) as fp:
            fp.write("herro!\n")
            fp.flush()

            self.assertTrue(os.path.exists(fp.name))

            repo = S3Repo()
            repo.create_repository()
            rf = repo.add_local_file(fp.name, move = False)
            repo.commit()

            self.assertTrue(os.path.exists(fp.name))
            self.assertTrue(os.path.exists(rf.local_path()))
            self.assertEqual(slurp(fp.name), "herro!\n")
            self.assertEqual(slurp(rf.local_path()), "herro!\n")
            repo.commit()

    def test_add_file_creates_repo_record(self):
        repo = S3Repo()
        repo.create_repository()
        repo.add_file(s3_bucket = '1', s3_key = '1')
        repo.add_file(s3_bucket = '1', s3_key = '2')
        repo.add_file(s3_bucket = '2', s3_key = '1')
        repo.add_file(s3_bucket = '2', s3_key = '3')
        repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_objects
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_bucket',  's3_key',  ],
            [ '1',          '1',       ],
            [ '1',          '2',       ],
            [ '2',          '1',       ],
            [ '2',          '3',       ],
        )

    def test_add_file_refuses_to_create_existing_file(self):
        repo = S3Repo()
        repo.create_repository()
        repo.add_file(s3_bucket = '1', s3_key = '1')
        with self.assertRaises(RepoFileAlreadyExistsError):
            repo.add_file(s3_bucket = '1', s3_key = '1')
        repo.commit()

    def test_publish_file_flags_repo_record(self):
        repo = S3Repo()
        repo.create_repository()
        rf1 = repo.add_file(s3_bucket = '1', s3_key = '1')
        rf2 = repo.add_file(s3_bucket = '1', s3_key = '2')
        repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_objects
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_bucket',  's3_key',  'published',  'date_published',  ],
            [ '1',          '1',       False,        None,              ],
            [ '1',          '2',       False,        None,              ],
        )

        rf1.publish()
        rf2.publish()
        rf1.update()
        rf2.update()
        repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_objects
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_bucket',  's3_key',  'published',  'date_published',  ],
            [ '1',          '1',       True,         now(),             ],
            [ '1',          '2',       True,         now(),             ],
        )


    @skip_unfinished
    def test_upload_stores_file_size(self):
        pass

    @skip_unfinished
    def test_lock_for_processing(self):
        pass

    @skip_unfinished
    def test_expire_flags_record(self):
        pass

    @skip_unfinished
    def test_purge_raises_error_if_published_file(self):
        pass

    @skip_unfinished
    def test_local_path(self):
        pass
