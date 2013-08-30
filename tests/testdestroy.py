from s3cache import S3Repo
import unittest

class DestroyTest(unittest.TestCase):
    @unittest.skip("---- Unwritten ----")
    def test_errors_if_does_not_exist(self):
        repo = S3Repo()
        repo.destroy_repository()

    @unittest.skip("---- Unwritten ----")
    def test_destroys_if_exists(self):
        pass

if __name__ == '__main__':
    unittest.main()

