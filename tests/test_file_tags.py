from s3repo import S3Repo
from s3repo.exceptions import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
from testcase import *

class FileTagsTest(DBTestCase):
    requires_online = False

    def test_tagging_files(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'abc', date_published = now())
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'def', date_published = now())
        rf1.tag_file('imported', 'processed')
        S3Repo.commit()

        self.assert_tags(
            [ 'tag_name',   ],
            [ 'imported',   ],
            [ 'processed',  ],
        )

        self.assert_rf_tags(
            [ 's3_key',    'tag_name',   ],
            [ rf1.s3_key,  'imported',   ],
            [ rf1.s3_key,  'processed',  ],
        )

    def test_untagging_files(self):
        rf1 = S3Repo.add_file(self.random_filename(), s3_key = 'abc', date_published = now())
        rf2 = S3Repo.add_file(self.random_filename(), s3_key = 'def', date_published = now())
        rf1.tag_file('imported', 'processed')
        rf2.tag_file('imported', 'processed')
        S3Repo.commit()

        rf1.untag_file('imported')
        S3Repo.commit()

        self.assert_rf_tags(
            [ 's3_key',    'tag_name',   ],
            [ rf1.s3_key,  'processed',  ],
            [ rf2.s3_key,  'imported',   ],
            [ rf2.s3_key,  'processed',  ],
        )

    def test_hour_tagging_files_is_default(self):
        rf = S3Repo.add_file(self.random_filename(), date_published = now())
        rf.tag_date(coerce_date('2013-04-24 01:02:03')) # Hour is is the default
        rf.commit()

        self.assert_tags(
            [ 'tag_name',                  ],
            [ 'day=2013-04-24',            ],
            [ 'hour=2013-04-24 01:00:00',  ],
            [ 'month=2013-04-01',          ],
            [ 'week=2013-04-22',           ],
        )

    def test_hour_tagging_files__creates_tags(self):
        rf = S3Repo.add_file(self.random_filename(), date_published = now())
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='hour')
        rf.commit()

        self.assert_tags(
            [ 'tag_name',                  ],
            [ 'day=2013-04-24',            ],
            [ 'hour=2013-04-24 01:00:00',  ],
            [ 'month=2013-04-01',          ],
            [ 'week=2013-04-22',           ],
        )

    def test_day_tagging_files__creates_tags(self):
        rf = S3Repo.add_file(self.random_filename(), date_published = now())
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='day')
        rf.commit()

        self.assert_tags(
            [ 'tag_name',          ],
            [ 'day=2013-04-24',    ],
            [ 'month=2013-04-01',  ],
            [ 'week=2013-04-22',   ],
        )

    def test_week_tagging_files__creates_tags(self):
        rf = S3Repo.add_file(self.random_filename(), date_published = now())
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='week')
        rf.commit()

        self.assert_tags(
            [ 'tag_name',          ],
            [ 'week=2013-04-22',   ],
        )

    def test_month_tagging_files__creates_tags(self):
        rf = S3Repo.add_file(self.random_filename(), date_published = now())
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='month')
        rf.commit()

        self.assert_tags(
            [ 'tag_name',          ],
            [ 'month=2013-04-01',  ],
        )

    def setup_default_tag_files(self, publish = True):
        rfs = [ S3Repo.add_file(self.random_filename()) for x in xrange(4) ]

        if publish:
            for rf in rfs:
                rf.publish()

        rfs[0].tag_file('imported', 'processed', 'archived')
        rfs[1].tag_file('imported', 'processed')
        rfs[2].tag_file('processed', 'restored', 'restricted')

        S3Repo.commit()
        # rfs[3] is untagged

        return rfs

    def test_find_tagged__all(self):
        rfs = self.setup_default_tag_files()

        tagged_files = S3Repo.find_tagged(all = [ 'imported', 'archived' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id })

        tagged_files = S3Repo.find_tagged(all = [ 'imported', 'processed' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id, rfs[1].file_id })

        tagged_files = S3Repo.find_tagged(all = [ 'imported' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id, rfs[1].file_id })

    def test_find_tagged__all_publish_filter__unpublished_files(self):
        rfs = self.setup_default_tag_files(False)
        tagged_files = S3Repo.find_tagged(all = [ 'imported', 'archived' ])
        self.assertEqual([ x.file_id for x in tagged_files ], [])

    def test_find_tagged__all_publish_filter__unpublished_files_all_view(self):
        rfs = self.setup_default_tag_files(False)
        tagged_files = S3Repo.find_tagged(all = [ 'imported', 'archived' ], published=False)
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id })

    def test_find_tagged__any(self):
        rfs = self.setup_default_tag_files()

        tagged_files = S3Repo.find_tagged(any = [ 'archived', 'restored' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id, rfs[2].file_id })

    def test_find_tagged__all_exclude(self):
        rfs = self.setup_default_tag_files()

        tagged_files = S3Repo.find_tagged(all = [ 'processed' ], exclude = [ 'restored' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id, rfs[1].file_id })

    def test_find_tagged__any_exclude(self):
        rfs = self.setup_default_tag_files()

        tagged_files = S3Repo.find_tagged(any = [ 'restored', 'archived' ], exclude = [ 'restricted' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id })

    def test_find_tagged__any_all(self):
        rfs = self.setup_default_tag_files()

        tagged_files = S3Repo.find_tagged(any = [ 'restored', 'archived' ], all = [ 'imported' ])
        self.assertEqual({ x.file_id for x in tagged_files }, { rfs[0].file_id })

    def test_find_tagged__exclude(self):
        with self.assertRaises(RepoAPIError):
            S3Repo.find_tagged(exclude = [ 'imported' ])

    def assert_tags(self, *rows):
        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.tags
            ORDER BY tag_name
        """, *rows)

    def assert_rf_tags(self, *rows):
        self.assertSqlResults(self.conn(), """
            SELECT *
            FROM s3_repo.all_file_tags
            ORDER BY s3_key, tag_name
        """, *rows)
