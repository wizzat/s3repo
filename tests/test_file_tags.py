from s3repo import *
from pyutil.testutil import *
from pyutil.dateutil import *
from pyutil.util import *
from testcase import *

class FileTagsTest(DBTestCase):
    requires_online = False

    def test_tagging_files(self):
        rf1 = self.repo.add_file(s3_key = 'abc')
        rf2 = self.repo.add_file(s3_key = 'def')
        rf1.tag('imported', 'processed')
        self.repo.commit()

        self.assert_tags(
            [ 'tag_id',     ],
            [ 'imported',   ],
            [ 'processed',  ],
        )

        self.assert_rf_tags(
            [ 's3_key',    'tag_id',     ],
            [ rf1.s3_key,  'imported',   ],
            [ rf1.s3_key,  'processed',  ],
            [ rf2.s3_key,  None,         ],
        )

    def test_untagging_files(self):
        rf1 = self.repo.add_file(s3_key = 'abc')
        rf2 = self.repo.add_file(s3_key = 'def')
        rf1.tag('imported', 'processed')
        rf2.tag('imported', 'processed')
        self.repo.commit()

        rf1.untag('imported')
        self.repo.commit()

        self.assert_rf_tags(
            [ 's3_key',    'tag_id',     ],
            [ rf1.s3_key,  'processed',  ],
            [ rf2.s3_key,  'imported',   ],
            [ rf2.s3_key,  'processed',  ],
        )

    def test_hour_tagging_files_is_default(self):
        rf = self.repo.add_file()
        rf.tag_date(coerce_date('2013-04-24 01:02:03')) # Hour is is the default
        rf.commit()

        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_tags
            ORDER BY tag_id
        """,
            [ 'tag_id',                    ],
            [ 'day=2013-04-24',            ],
            [ 'hour=2013-04-24 01:00:00',  ],
            [ 'month=2013-04-01',          ],
            [ 'week=2013-04-22',           ],
        )

    def test_hour_tagging_files__creates_tags(self):
        rf = self.repo.add_file()
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='hour')
        rf.commit()

        self.assert_tags(
            [ 'tag_id',                    ],
            [ 'day=2013-04-24',            ],
            [ 'hour=2013-04-24 01:00:00',  ],
            [ 'month=2013-04-01',          ],
            [ 'week=2013-04-22',           ],
        )

    def test_day_tagging_files__creates_tags(self):
        rf = self.repo.add_file()
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='day')
        rf.commit()

        self.assert_tags(
            [ 'tag_id',            ],
            [ 'day=2013-04-24',    ],
            [ 'month=2013-04-01',  ],
            [ 'week=2013-04-22',   ],
        )

    def test_week_tagging_files__creates_tags(self):
        rf = self.repo.add_file()
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='week')
        rf.commit()

        self.assert_tags(
            [ 'tag_id',            ],
            [ 'week=2013-04-22',   ],
        )

    def test_month_tagging_files__creates_tags(self):
        rf = self.repo.add_file()
        rf.tag_date(coerce_date('2013-04-24 01:02:03'), type='month')
        rf.commit()

        self.assert_tags(
            [ 'tag_id',            ],
            [ 'month=2013-04-01',  ],
        )

    def setup_default_tag_files(self):
        rfs = [ self.repo.add_file() for x in xrange(4) ]
        rfs[0].tag('imported', 'processed', 'archived')
        rfs[1].tag('imported', 'processed')
        rfs[2].tag('processed', 'restored', 'restricted')
        # rfs[3] is untagged

        return rfs

    def test_find_tagged__all(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(all = [ 'imported', 'archived' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no })

        tagged_files = self.repo.find_tagged(all = [ 'imported', 'processed' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no, rfs[1].file_no })

        tagged_files = self.repo.find_tagged(all = [ 'imported' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no, rfs[1].file_no })

    def test_find_tagged__any(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(any = [ 'archived', 'restored' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no, rfs[2].file_no })

    def test_find_tagged__exclude(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(exclude = [ 'restored' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no, rfs[1].file_no, rfs[3].file_no })

    def test_find_tagged__all_exclude(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(all = [ 'processed' ], exclude = [ 'restored' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no, rfs[1].file_no })

    def test_find_tagged__any_exclude(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(any = [ 'restored', 'archived' ], exclude = [ 'restricted' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no })

    def test_find_tagged__any_all(self):
        rfs = self.setup_default_tag_files()

        tagged_files = self.repo.find_tagged(any = [ 'restored', 'archived' ], all = [ 'imported' ])
        self.assertEqual({ x.file_no for x in tagged_files }, { rfs[0].file_no })

    def assert_tags(self, *rows):
        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_tags
            ORDER BY tag_id
        """, *rows)

    def assert_rf_tags(self, *rows):
        self.assertSqlResults(self.conn, """
            SELECT *
            FROM s3_repo
                LEFT OUTER JOIN s3_repo_tags USING (file_no)
                LEFT OUTER JOIN s3_tags USING (tag_no)
            ORDER BY s3_key, tag_id
        """, *rows)
