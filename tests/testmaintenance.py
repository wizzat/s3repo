from s3cache import S3Repo
import unittest

class MaintenanceTest(unittest.TestCase):
    @unittest.skip("---- Unwritten ----")
    def test_expired_records_purged_from_s3(self):
        pass

    @unittest.skip("---- Unwritten ----")
    def test_expired_records_are_kept_until_repo_backup(self):
        pass

    @unittest.skip("---- Unwritten ----")
    def test_restore_from_backup(self):
        pass

    @unittest.skip("---- Unwritten ----")
    def test_save_backup(self):
        pass

if __name__ == '__main__':
    unittest.main()
