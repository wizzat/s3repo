import unittest, psycopg2, json, os
import s3repo.host
import s3repo.common
from testcase import DBTestCase
from s3repo import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *

class RepoTest(DBTestCase):
    def test_maintain_database_does_not_delete_published_files(self):
        current_host = s3repo.host.RepoHost.current_host_id()

        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        rf1.publish()
        rf1.unlink()
        rf2.unlink()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
                LEFT OUTER JOIN s3_repo.downloads
                    USING (file_id)
            ORDER BY s3_key
        """,
            [ 'file_id',    's3_key',  'date_published',  'published',  'host_id',     ],
            [ rf1.file_id,  'abc',     now(),             True,         None,          ], # Published but doesnt exist anywhere.  Should still exist
            [ rf2.file_id,  'def',     None,              False,        None,          ], # Unpublished and exists nowhere. Should be deleted
            [ rf3.file_id,  'ghi',     None,              False,        current_host,  ], # Unpublished, but still exists somewhere. Should still exist
        )

        S3Repo.maintain_database()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
                LEFT OUTER JOIN s3_repo.downloads
                    USING (file_id)
            ORDER BY s3_key
        """,
            [ 'file_id',    's3_key',  'date_published',  'published',  'host_id',     ],
            [ rf1.file_id,  'abc',     now(),             True,         None,          ], # Published but doesnt exist anywhere.  Should still exist
            [ rf3.file_id,  'ghi',     None,              False,        current_host,  ], # Unpublished, but still exists somewhere. Should still exist
        )

    def test_unpublished_files_are_only_removed_for_locally_created_content(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        for rf in [ rf1, rf2, rf3 ]:
            rf.date_created = now() - weeks(2)
            rf.update()

        rf3.origin = s3repo.host.RepoHost.find_or_create('abc').host_id
        rf3.update()
        S3Repo.commit()

        S3Repo.maintain_current_host()

        self.assertFalse(os.path.exists(rf1.local_path()))
        self.assertFalse(os.path.exists(rf2.local_path()))
        self.assertTrue(os.path.exists(rf3.local_path()))
        S3Repo.commit()

    def test_timelimit_for_deleting_unpublished_files(self):
        current_host = s3repo.host.RepoHost.current_host_id()
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = "abc")
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = "def")
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = "ghi")

        dt = now() - seconds(self.config['fs.unpublished_stale_seconds']) - seconds(2)
        for i, rf in enumerate([ rf1, rf2, rf3 ]):
            rf.date_created = dt + seconds(i)
            rf.update()

        S3Repo.maintain_current_host()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
                LEFT OUTER JOIN s3_repo.downloads
                    USING (file_id)
            ORDER BY s3_key
        """,
            [ 'file_id',    'date_created',    'host_id',     ],
            [ rf1.file_id,  rf1.date_created,  None,          ],
            [ rf2.file_id,  rf2.date_created,  None,          ],
            [ rf3.file_id,  rf3.date_created,  current_host,  ],
        )
