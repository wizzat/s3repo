from s3cache import S3Repo
import unittest

class FileManagementTest(unittest.TestCase):
    @unittest.skip("---- Unwritten ----")
    def test_add_file_copies_file_into_local_cache(self):
        pass

    def test_add_file_creates_repo_record(self):
        pass

    def test_publish_file_flags_repo_record(self):
        pass

    def test_publish_uploads_file(self):
        pass

    def test_upload_puts_files_on_s3(self):
        pass

    def test_upload_stores_md5(self):
        pass

    def test_upload_stores_file_size(self):
        pass

    def test_lock_for_processing(self):
        pass

    def test_expire_flags_record(self):
        pass

if __name__ == '__main__':
    unittest.main()


