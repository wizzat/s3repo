import unittest, psycopg2, json, os, time
from testcase import DBTestCase
from s3repo import S3Repo
from pyutil.pghelper import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *

class S3RepoIntegrationTest(DBTestCase):
    requires_online = True

    @skip_unfinished
    def test_save_backup(self):
        rf1 = S3Repo.add_file(s3_key = "abc")
        rf2 = S3Repo.add_file(s3_key = "bcd")
        rf3 = S3Repo.add_file(s3_key = "cde")
        rf4 = S3Repo.backup_db()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo
            ORDER BY s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               rf4.file_size,  ],
        )

    @skip_unfinished
    def test_restore_backup(self):
        rf1 = S3Repo.add_file(s3_key = "abc")
        rf2 = S3Repo.add_file(s3_key = "bcd")
        rf3 = S3Repo.add_file(s3_key = "cde")
        rf4 = S3Repo.backup_db()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo
            ORDER BY s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               rf4.file_size,  ],
        )
        self.conn.commit()

        S3Repo.destroy_repository()
        S3Repo.commit()
        S3Repo = s3repo.S3Repo()
        S3Repo.restore_db()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo
            ORDER BY s3_key
        """,
            [ 'file_no',    's3_bucket',    's3_key',    'published',    'date_published',    'file_size',    ],
            [ rf1.file_no,  rf1.s3_bucket,  rf1.s3_key,  rf1.published,  rf1.date_published,  rf1.file_size,  ],
            [ rf2.file_no,  rf2.s3_bucket,  rf2.s3_key,  rf2.published,  rf2.date_published,  rf2.file_size,  ],
            [ rf3.file_no,  rf3.s3_bucket,  rf3.s3_key,  rf3.published,  rf3.date_published,  rf3.file_size,  ],
            [ rf4.file_no,  rf4.s3_bucket,  rf4.s3_key,  True,           now(),               -1,             ],
        )
