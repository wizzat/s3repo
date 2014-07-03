import tempfile, zlib, base64, hashlib
import pyutil.pghelper
from s3repo.exceptions import *
from s3repo import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
from testcase import *

class FileIntegrationTest(DBTestCase):
    requires_online = True

    @skip_unfinished
    def test_upload(self):
        contents = 'something bad goes here\n'
        rf1 = S3Repo.add_file(self.random_filename(contents))
        rf1.upload()
        S3Repo.commit()

        self.assertEqual(rf1.md5, hashlib.md5(contents).hexdigest())
        self.assertEqual(rf1.b64, base64.b64encode(hashlib.md5(contents).digest()))
        self.assertEqual(rf1.file_size, len(contents))
        self.assertEqual(set(self.s3_list_bucket(rf1.s3_bucket())), { rf1.s3_key })

    def test_download_checks_md5(self):
        rf1 = S3Repo.add_file(self.random_filename('something bad goes here\n'))
        rf1.upload()
        S3Repo.commit()

        os.unlink(rf1.local_path())
        rf1.md5 = 'abc'
        rf1.update()

        with self.assertRaises(RepoDownloadError):
            rf1.download()

    def test_purge_removes_from_s3(self):
        rf1 = S3Repo.add_file(self.random_filename('some contents'))
        rf1.upload()
        rf1.purge()
        S3Repo.commit()

        bucket = self.s3_conn.get_bucket(self.config['s3.default_bucket'])
        self.assertEqual(self.s3_list_bucket(rf1.s3_bucket()), [])
