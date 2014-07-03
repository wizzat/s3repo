import tempfile, zlib, base64, hashlib
import pyutil.pghelper
from s3repo.exceptions import *
from s3repo import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
from testcase import *


class FileTest(DBTestCase):
    def test_add_file_creates_repo_record(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_bucket = '1', s3_key = '1')
        rf2 = S3Repo.add_file(self.random_filename(), s3_bucket = '1', s3_key = '2')
        rf3 = S3Repo.add_file(self.random_filename(), s3_bucket = '2', s3_key = '1')
        rf4 = S3Repo.add_file(self.random_filename(), s3_bucket = '2', s3_key = '3')
        S3Repo.commit()

        self.assertEqual(len({ x.file_id for x in [ rf1, rf2, rf3, rf4 ] }), 4)

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
                INNER JOIN s3_repo.s3_buckets USING (s3_bucket_id)
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_id',    's3_bucket',  's3_key',  ],
            [ rf1.file_id,  '1',          '1',       ],
            [ rf2.file_id,  '1',          '2',       ],
            [ rf3.file_id,  '2',          '1',       ],
            [ rf4.file_id,  '2',          '3',       ],
        )

    def test_add_file_refuses_to_create_existing_s3_key(self):
        filename = self.random_filename()
        S3Repo.add_file(filename, s3_bucket = '1', s3_key = '1')

        with self.assertRaises(RepoConcurrentInsertionError):
            S3Repo.add_file(filename, s3_bucket = '1', s3_key = '1')

        S3Repo.commit()

    def test_add_file_mvcc(self):
        filename = self.random_filename()

        set_now(123)
        rf1 = S3Repo.add_file(filename, s3_key = 'f1')
        rf1.publish()
        S3Repo.commit()

        set_now(124)
        rf2 = S3Repo.add_file(filename, s3_key = 'f2')
        rf2.publish()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
                LEFT OUTER JOIN s3_repo.paths USING (path_id)
            ORDER BY s3_key
        """,
            [ 'local_path',  's3_key',  'published',  'date_published',  ],
            [ filename,      'f1',       True,         from_epoch(123),   ],
            [ filename,      'f2',       True,         from_epoch(124),   ],
        )


    def test_publish_file_flags_repo_record(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = '1')
        rf2 = S3Repo.add_file(self.random_filename() + '.gz', s3_key = '2')
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'md5',  'file_size',  ],
            [ '1',       False,        None,              None,   None,         ],
            [ '2',       False,        None,              None,   None,         ],
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
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'md5',                                 'file_size',       ],
            [ '1',       True,         now(),             hashlib.md5(f1_contents).hexdigest(),  len(f1_contents),  ],
            [ '2',       True,         now(),             hashlib.md5(f2_contents).hexdigest(),  len(f2_contents),  ],
        )

    def test_expire_flags_record(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'unpublished')
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'published')
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = 'control')

        rf2.publish()
        rf3.publish()
        rf1.expire()
        rf2.expire()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',       'published',  'date_published',  'date_expired',  ],
            [ 'control',      True,         now(),             None,            ],
            [ 'published',    False,        now(),             now(),           ],
            [ 'unpublished',  False,        None,              now(),           ],
        )

    def test_purge_raises_error_if_published_file(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'unpublished')
        rf1.publish()

        with self.assertRaises(PurgingPublishedRecordError):
            rf1.purge()

    def test_lock_for_processing(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'unpublished')
        S3Repo.commit()
        rf1.rowlock()
        rf1.rowlock()

        with self.assertRaises(pyutil.pghelper.PgOperationalError):
            execute(self.conn(), "select * from s3_repo.files for update nowait")
        S3Repo.commit()

    def test_publishing_an_expired_file(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'f1')
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'f2')

        for rf in [ rf1, rf2 ]:
            rf.touch()
            rf.publish()
            rf.expire()
            rf.update()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      False,        now(),             now(),           ],
            [ 'f2',      False,        now(),             now(),           ],
        )

        rf1.publish()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      True,         now(),             None,            ],
            [ 'f2',      False,        now(),             now(),           ],
        )

    def test_expiring_unpublished_file(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'f1')
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'f2')
        rf1.touch()
        rf1.expire()

        rf2.touch()
        rf2.publish()
        rf2.expire()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      False,        None,              now(),           ],
            [ 'f2',      False,        now(),             now(),           ],
        )

    def test_republishing_published_files_does_not_change_date_published(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'f1')
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'f2')
        rf3 = S3Repo.add_file(self.random_filename(), s3_key = 'f3')

        rf1.publish()
        rf2.publish()
        S3Repo.commit()

        t1 = now()
        reset_now()
        t2 = now()

        rf2.publish()
        rf3.publish()
        S3Repo.commit()

        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.files
            ORDER BY s3_key
        """,
            [ 's3_key',  'published',  'date_published',  ],
            [ 'f1',      True,         t1,                ],
            [ 'f2',      True,         t1,                ],
            [ 'f3',      True,         t2,                ],
        )
