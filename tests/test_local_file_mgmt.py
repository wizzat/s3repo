import unittest, tempfile
from s3cache import S3Repo
from pyutil.testutil import *
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


    @skip_unfinished
    def test_add_file_creates_repo_record(self):
        pass

    @skip_unfinished
    def test_publish_file_flags_repo_record(self):
        pass

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
