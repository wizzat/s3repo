import tempfile, md5, zlib, base64
from s3repo import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
from s3exceptions import *
from testcase import *

class RemoteFileMgmtTest(DBTestCase):
    requires_online = True

    @skip_offline
    def test_publish_uploads_file(self):
        rf1 = self.repo.add_file()
        rf1.touch()
        rf1.publish()

        bucket = self.s3_conn.get_bucket(self.config['default_s3_bucket'])
        self.assertEqual({ x.name for x in bucket.list() }, { rf1.s3_key })

    @skip_offline
    def test_upload_puts_files_on_s3(self):
        rf1 = self.repo.add_file()
        rf1.touch()
        rf1.upload()

        bucket = self.s3_conn.get_bucket(self.config['default_s3_bucket'])
        self.assertEqual({ x.name for x in bucket.list() }, { rf1.s3_key })

    @skip_offline
    def test_upload_stores_md5(self):
        rf1 = self.repo.add_file()

        contents = 'something bad goes here\n'
        with rf1.open('w') as fp:
            fp.write(contents)

        rf1.upload()
        self.assertEqual(rf1.md5, md5.md5(contents).hexdigest())
        self.assertEqual(rf1.b64, base64.b64encode(md5.md5(contents).digest()))
        self.assertEqual(rf1.file_size, len(contents))

    @skip_offline
    def test_download_checks_md5(self):
        rf1 = self.repo.add_file(md5 = md5.md5('something bad goes here').hexdigest())
        rf1.touch()
        rf1.upload()

        os.unlink(rf1.local_path())
        rf1.md5 = 'abc'
        rf1.update()

        with self.assertRaises(RepoDownloadError):
            rf1.download()

    @skip_offline
    def test_purge_removes_from_s3(self):
        rf1 = self.repo.add_file()
        rf1.touch()
        rf1.upload()
        rf1.purge()

        bucket = self.s3_conn.get_bucket(self.config['default_s3_bucket'])
        self.assertEqual([ x.name for x in bucket.list() ], [])

class LocalFileMgmtTest(DBTestCase):
    def test_add_file_moves_file_into_local_cache(self):
        filename = None
        with tempfile.NamedTemporaryFile(delete = False) as fp:
            fp.write("herro!\n")
            fp.flush()

            self.assertTrue(os.path.exists(fp.name))

            rf = self.repo.add_local_file(fp.name)
            self.repo.commit()

            self.assertFalse(os.path.exists(fp.name))
            self.assertTrue(os.path.exists(rf.local_path()))
            self.assertEqual(slurp(rf.local_path()), "herro!\n")

    def test_add_file_copies_file_into_local_cache(self):
        filename = None
        with tempfile.NamedTemporaryFile(delete = True) as fp:
            fp.write("herro!\n")
            fp.flush()

            self.assertTrue(os.path.exists(fp.name))

            rf = self.repo.add_local_file(fp.name, move = False)
            self.repo.commit()

            self.assertTrue(os.path.exists(fp.name))
            self.assertTrue(os.path.exists(rf.local_path()))
            self.assertEqual(slurp(fp.name), "herro!\n")
            self.assertEqual(slurp(rf.local_path()), "herro!\n")
            self.repo.commit()

    def test_add_file_creates_repo_record(self):
        rf1 = self.repo.add_file(s3_bucket = '1', s3_key = '1')
        rf2 = self.repo.add_file(s3_bucket = '1', s3_key = '2')
        rf3 = self.repo.add_file(s3_bucket = '2', s3_key = '1')
        rf4 = self.repo.add_file(s3_bucket = '2', s3_key = '3')
        self.repo.commit()

        self.assertEqual(len({ x.file_no for x in [ rf1, rf2, rf3, rf4 ] }), 4)

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 'file_no',    's3_bucket',  's3_key',  ],
            [ rf1.file_no,  '1',          '1',       ],
            [ rf2.file_no,  '1',          '2',       ],
            [ rf3.file_no,  '2',          '1',       ],
            [ rf4.file_no,  '2',          '3',       ],
        )

    def test_add_file_refuses_to_create_existing_file(self):
        self.repo.add_file(s3_bucket = '1', s3_key = '1')
        with self.assertRaises(RepoFileAlreadyExistsError):
            self.repo.add_file(s3_bucket = '1', s3_key = '1')
        self.repo.commit()

    def test_publish_file_flags_repo_record(self):
        rf1 = self.repo.add_file(s3_key = '1')
        rf2 = self.repo.add_file(s3_key = '2')
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
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
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'md5',                             'file_size',       ],
            [ '1',       True,         now(),             md5.md5(f1_contents).hexdigest(),  len(f1_contents),  ],
            [ '2',       True,         now(),             md5.md5(f2_contents).hexdigest(),  len(f2_contents),  ],
        )

    def test_expire_flags_record(self):
        rf1 = self.repo.add_file(s3_key = 'unpublished')
        rf2 = self.repo.add_file(s3_key = 'published')
        rf3 = self.repo.add_file(s3_key = 'control')

        rf1.touch()
        rf2.touch()
        rf3.touch()

        rf2.publish()
        rf3.publish()
        rf1.expire()
        rf2.expire()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',       'published',  'date_published',  'date_expired',  ],
            [ 'control',      True,         now(),             None,            ],
            [ 'published',    False,        now(),             now(),           ],
            [ 'unpublished',  False,        None,              now(),           ],
        )

    def test_purge_raises_error_if_published_file(self):
        rf1 = self.repo.add_file(s3_key = 'unpublished')
        rf1.touch()
        rf1.publish()
        with self.assertRaises(PurgingPublishedRecordError):
            rf1.purge()
        self.repo.commit()

    def test_local_path(self):
        rf = self.repo.add_file(s3_bucket = 'abc', s3_key = 'def')
        self.repo.commit()
        self.assertEqual(rf.local_path(), '/mnt/s3repo/abc/def')

    def test_lock_for_processing(self):
        rf1 = self.repo.add_file(s3_key = 'unpublished')
        self.repo.commit()
        rf1.rowlock()
        rf1.rowlock()

        with self.assertRaises(psycopg2.OperationalError):
            execute(self.conn, "select * from s3_repo for update nowait")
        self.repo.commit()

    def test_find_by_delegation(self):
        rf1 = self.repo.add_file(s3_bucket = '1', s3_key = 'f1')
        rf2 = self.repo.add_file(s3_bucket = '1', s3_key = 'f2')
        rf3 = self.repo.add_file(s3_bucket = '1', s3_key = 'f3')
        rf4 = self.repo.add_file(s3_bucket = '2', s3_key = 'f4')

        self.assertEqual({ x.file_no for x in self.repo.find_by(s3_key = 'f2') }, { rf2.file_no })
        self.assertEqual({ x.file_no for x in self.repo.find_by(s3_bucket = '1') }, { rf1.file_no, rf2.file_no, rf3.file_no })
        self.assertEqual({ x.file_no for x in self.repo.find_by(s3_bucket = '2') }, { rf4.file_no })
        self.assertEqual({ x.file_no for x in self.repo.find_by(s3_bucket = '3') }, set())
        self.repo.commit()

    def test_publishing_an_expired_file(self):
        rf1 = self.repo.add_file(s3_key = 'f1')
        rf2 = self.repo.add_file(s3_key = 'f2')

        for rf in [ rf1, rf2 ]:
            rf.touch()
            rf.publish()
            rf.expire()
            rf.update()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      False,        now(),             now(),           ],
            [ 'f2',      False,        now(),             now(),           ],
        )

        rf1.publish()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      True,         now(),             None,            ],
            [ 'f2',      False,        now(),             now(),           ],
        )

    def test_expiring_unpublished_file(self):
        rf1 = self.repo.add_file(s3_key = 'f1')
        rf2 = self.repo.add_file(s3_key = 'f2')
        rf1.touch()
        rf1.expire()

        rf2.touch()
        rf2.publish()
        rf2.expire()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',  'published',  'date_published',  'date_expired',  ],
            [ 'f1',      False,        None,              now(),           ],
            [ 'f2',      False,        now(),             now(),           ],
        )

    def test_republishing_published_files_does_not_change_date_published(self):
        t1 = now()
        rf1 = self.repo.add_file(s3_key = 'f1')
        rf2 = self.repo.add_file(s3_key = 'f2')
        rf3 = self.repo.add_file(s3_key = 'f3')

        for rf in [ rf1, rf2 ]:
            rf.touch()
            rf.publish()
        self.repo.commit()

        t1 = now()
        reset_now()
        t2 = now()

        rf2.publish()
        rf3.touch()
        rf3.publish()
        self.repo.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
            ORDER BY s3_bucket, s3_key
        """,
            [ 's3_key',  'published',  'date_published',  ],
            [ 'f1',      True,         t1,                ],
            [ 'f2',      True,         t1,                ],
            [ 'f3',      True,         t2,                ],
        )
