from s3cache import S3Repo
import unittest

class CreateTest(unittest.TestCase):
    @unittest.skip("---- Unwritten ----")
    def test_creates(self):
        repo = S3Repo()
        repo.create_repository()

    @unittest.skip("---- Unwritten ----")
    def test_errors_if_already_exists(self):
        pass

if __name__ == '__main__':
    unittest.main()
