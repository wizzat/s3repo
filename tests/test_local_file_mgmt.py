import unittest, tempfile, md5, zlib
from s3cache import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
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
            [ 's3_bucket',  's3_key',  'published',  'date_published',  'md5',  'file_size',  ],
            [ '1',          '1',       False,        None,              None,   None,         ],
            [ '1',          '2',       False,        None,              None,   None,         ],
        )

        f1_contents = "yakkety yak, don't talk back"
        f2_contents = zlib.compress("take out the papers and the trash")
        mkdirp(os.path.dirname(rf1.local_path()))
        with open(rf1.local_path(), 'w') as fp:
            fp.write(f1_contents)

        with open(rf2.local_path(), 'w') as fp:
            fp.write(f2_contents)


        rf1.publish()
        rf2.publish()
        repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_objects
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_bucket',  's3_key',  'published',  'date_published',  'md5',                             'file_size',       ],
            [ '1',          '1',       True,         now(),             md5.md5(f1_contents).hexdigest(),  len(f1_contents),  ],
            [ '1',          '2',       True,         now(),             md5.md5(f2_contents).hexdigest(),  len(f2_contents),  ],
        )

    def test_expire_flags_record(self):
        repo = S3Repo().create_repository()
        rf1 = repo.add_file(s3_key = 'unpublished')
        rf2 = repo.add_file(s3_key = 'published')
        rf3 = repo.add_file(s3_key = 'control')

        rf1.touch()
        rf2.touch()
        rf3.touch()

        rf2.publish()
        rf3.publish()
        rf1.expire()
        rf2.expire()
        repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_objects
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',       'published',  'date_published',  'date_expired',  ],
            [ 'control',      True,         now(),             None,            ],
            [ 'published',    False,        now(),             now(),           ],
            [ 'unpublished',  False,        None,              now(),           ],
        )

    def test_purge_raises_error_if_published_file(self):
        repo = S3Repo().create_repository()
        rf1 = repo.add_file(s3_key = 'unpublished')
        rf1.touch()
        rf1.publish()
        with self.assertRaises(PurgingPublishedRecordError):
            rf1.purge()
        repo.commit()

    def test_local_path(self):
        repo = S3Repo().create_repository()
        rf = repo.add_file(s3_bucket = 'abc', s3_key = 'def')
        repo.commit()
        self.assertEqual(rf.local_path(), '/mnt/s3cache/abc/def')

    @skip_unfinished
    def test_lock_for_processing(self):
        pass

