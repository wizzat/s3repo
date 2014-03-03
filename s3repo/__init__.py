import os, shutil, md5, uuid, socket, subprocess, gzip, boto
from pyutil.pghelper import *
from pyutil.util import *
from pyutil.dateutil import *
from pyutil.decorators import *
from boto.s3.key import Key, compute_md5
from s3repo.exceptions import *

__all__ = [
    'S3Repo',
    'raw_cfg',
]

@memoize()
def raw_cfg():
    return load_json_paths(
        os.environ.get('S3_REPO_CFG', None),
        '~/.s3_repo_cfg',
    )

class S3Repo(object):
    def __init__(self):
        self.s3_buckets = {}
        self.config     = raw_cfg()
        self.db_mgr     = ConnMgr(**self.config['database'])
        self.db_conn    = self.db_mgr.getconn("conn")
        self.s3_conn    = boto.connect_s3(
            self.config['s3_access_key'],
            self.config['s3_secret_key'],
        )

        class Tags(_Tag):
            conn = self.db_conn

        class RepoFileTags(_RepoFileTags):
            conn = self.db_conn

        class RepoFile(_RepoFile):
            local_root  = self.config['local_root']
            conn        = self.db_conn
            s3_conn     = self.s3_conn
            Tag         = Tags
            RepoFileTag = RepoFileTags



        self.RepoFile = RepoFile
        self.Tag = Tags
        self.RepoFileTag= RepoFileTags

        self.commit = self.db_conn.commit
        self.rollback = self.db_conn.rollback
        self.find_by = self.RepoFile.find_by

    def create_repository(self, error = True):
        if table_exists(self.db_conn, "s3_repo"):
            if error:
                raise RepoAlreadyExistsError()
            else:
                return

        execute(self.db_conn, "CREATE SEQUENCE s3_repo_seq")
        execute(self.db_conn, "CREATE SEQUENCE s3_tag_seq")
        execute(self.db_conn, """
            CREATE TABLE s3_repo (
                file_no          INTEGER PRIMARY KEY DEFAULT nextval('s3_repo_seq'),
                s3_bucket        VARCHAR(64),
                s3_key           VARCHAR(1024),
                origin           TEXT,
                md5              TEXT,
                b64              TEXT,
                file_size        INTEGER,
                date_created     TIMESTAMP DEFAULT now(),
                date_uploaded    TIMESTAMP,
                date_published   TIMESTAMP,
                date_archived    TIMESTAMP,
                date_expired     TIMESTAMP,
                published        BOOLEAN DEFAULT FALSE
            )
        """)

        execute(self.db_conn, """
            CREATE TABLE s3_tags (
                tag_no INTEGER PRIMARY KEY DEFAULT nextval('s3_tag_seq'),
                tag_id TEXT
            )
        """)

        execute(self.db_conn, """
            CREATE TABLE s3_repo_tags (
                file_no     INTEGER NOT NULL REFERENCES s3_repo (file_no),
                tag_no      INTEGER NOT NULL REFERENCES s3_tags (tag_no),
                date_tagged TIMESTAMP,
                --
                PRIMARY KEY (file_no, tag_no)
            )
        """)

        execute(self.db_conn, "CREATE UNIQUE INDEX unq_s3_bucket_key ON s3_repo (s3_bucket, s3_key)")
        self.db_conn.commit()
        return self

    def flush_repository(self):
        execute(self.db_conn, """
            TRUNCATE TABLE s3_repo_tags, s3_tags, s3_repo;
        """)

    def destroy_repository(self):
        import psycopg2

        try:
            execute(self.db_conn, "DROP TABLE IF EXISTS s3_repo_tags")
            execute(self.db_conn, "DROP TABLE IF EXISTS s3_repo")
            execute(self.db_conn, "DROP TABLE IF EXISTS s3_tags")
            execute(self.db_conn, "DROP SEQUENCE s3_tag_seq")
            execute(self.db_conn, "DROP SEQUENCE s3_repo_seq")
            self.db_conn.commit()
            return True
        except psycopg2.ProgrammingError:
            self.db_conn.rollback()
            return False

    def add_local_file(self, local_path, s3_key = None, s3_bucket = None, move = True, **kwargs):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or os.path.basename(local_path)
        rf = self.add_file(s3_key, s3_bucket, **kwargs)

        mkdirp(os.path.dirname(rf.local_path()))
        if move:
            shutil.move(local_path, rf.local_path())
        else:
            shutil.copy(local_path, rf.local_path())

        return rf

    def add_file(self, s3_key = None, s3_bucket = None, **kwargs):
        s3_bucket = s3_bucket or self.config['default_s3_bucket']
        s3_key = s3_key or str(uuid.uuid4())

        rf = list(self.RepoFile.find_by(s3_key = s3_key, s3_bucket = s3_bucket))
        if rf:
            raise RepoFileAlreadyExistsError(repr(rf))

        return self.RepoFile(
            s3_bucket = s3_bucket,
            s3_key    = s3_key,
            origin    = socket.gethostname(),
            **kwargs
        ).update()

    def backup_db(self):
        """
        Creates a backup of the S3 Cache and uploads it to config['backup_s3_bucket']/s3repo_backups
        Backup name will be: "YYYY-MM-DD_HH:24:MI:SS.sql.gz"
        Ensures that no more than config['num_backups'] exist.
        """
        backup_file = self.add_file(
            s3_bucket      = self.config['backup_s3_bucket'],
            s3_key         = now().strftime("s3repo_backups/%Y%m%d%H%M%S.sql.gz"),
            date_published = now(),
            published      = True,
        )

        with backup_file.open('w') as fp:
            self.db_conn.cursor().copy_to(fp, 's3_repo', columns = self.RepoFile.fields)

        backup_file.publish()

        return backup_file

    def restore_db(self):
        """
        Queries config['backup_s3_bucket']/s3repo_backups and restores the latest backup.
        """
        backup_bucket = self.s3_conn.get_bucket(self.config['backup_s3_bucket'])
        remote_backup_files = backup_bucket.list("s3repo_backups")
        self.create_repository()

        try:
            last_backup = sorted(remote_backup_files, key=lambda x: x.name)[-1]
        except IndexError:
            raise RepoNoBackupsError()

        local_path = os.path.join(
            self.config['local_root'],
            self.config['backup_s3_bucket'],
            last_backup.name,
        )

        last_backup.get_contents_to_filename(local_path)

        with gzip.open(local_path, 'r') as fp:
            self.db_conn.cursor().copy_from(fp, 's3_repo', columns = self.RepoFile.fields)

        rf = list(self.RepoFile.find_by(
            s3_bucket = self.config['backup_s3_bucket'],
            s3_key    = last_backup.name,
        ))[0]
        rf.file_size = -1
        rf.update()

        self.db_conn.commit()
        return last_backup

    def cleanup_unpublished_files(self):
        """
        Purges local files which have not been published
        """
        unpublished_files = self.find_by(origin = socket.gethostname(), published = False, date_published = None)

        for rf in unpublished_files:
            if rf.date_created < now() - weeks(1):
                rf.purge()

    def cleanup_local_disk(self):
        """
        Recursively examines config['local_root'] and unlinks files which have been accessed more than config['local_atime_limit'] minutes ago.
        """
        raise NotImplemented()

    def find_tagged(self, any = None, all = None, exclude = None):
        """
        """
        all     = all or []
        any     = any or []
        exclude = exclude or []
        tags    = any + all + exclude

        any_ct      = ""
        all_ct      = ""
        exclude_ct  = ""
        filter_hint = ""

        if all:
            all_ct = "AND sum(CASE WHEN tag_id IN %(all_tags)s THEN 1 ELSE 0 END) = %(num_all_tags)s"

        if any:
            any_ct = "AND sum(case when tag_id in %(any_tags)s then 1 else 0 end) >= 1"

        if exclude:
            exclude_ct = "AND sum(case when tag_id in %(exclude_tags)s then 1 else 0 end) = 0"

        if any or all:
            filter_hint = "AND tag_id IN %(tags)s"

        query = """
            SELECT file_no
            FROM s3_repo r
                LEFT OUTER JOIN s3_repo_tags rt USING (file_no)
                LEFT OUTER JOIN s3_tags USING (tag_no)
            WHERE r.date_published IS NOT NULL
                AND r.date_expired IS NULL
                {filter_hint}
            GROUP BY file_no
            HAVING true
                {exclude_ct}
                {any_ct}
                {all_ct}
        """.format(
            any_ct      = any_ct,
            all_ct      = all_ct,
            exclude_ct  = exclude_ct,
            filter_hint = filter_hint,
        )

        results = fetch_results(self.db_conn, query,
            any_tags     = tuple(any),
            all_tags     = tuple(all),
            exclude_tags = tuple(exclude),
            num_all_tags = len(all),
            tags         = tuple(tags),
        )

        return self.RepoFile.find_by(file_no = tuple(x['file_no'] for x in results))

class _RepoFile(DBTable):
    table_name = 's3_repo'
    key_field  = 'file_no'
    db_conn    = None
    s3_conn    = None
    fields     = (
        'file_no',
        's3_bucket',
        's3_key',
        'published',
        'origin',
        'md5',
        'b64',
        'file_size',
        'date_created',
        'date_uploaded',
        'date_published',
        'date_archived',
        'date_expired',
    )

    def s3_path(self):
        return "s3://{}/{}".format(self.s3_bucket, self.s3_key)

    def local_path(self):
        return os.path.join(
            self.local_root,
            self.s3_bucket,
            self.s3_key,
        )

    def publish(self):
        if self.date_expired or not self.published:
            self.published = True
            self.date_expired = None
            self.date_published = now()
        self.upload()
        self.update()

    def expire(self):
        self.published = False
        if not self.date_expired:
            self.date_expired = now()
        self.update()

    def upload(self):
        if self.date_uploaded:
            return

        if not os.path.exists(self.local_path()):
            raise RepoFileDoesNotExistLocallyError()

        if not self.file_size:
            with open(self.local_path(), 'r') as fp:
                self.md5, self.b64, self.file_size = compute_md5(fp)

        if is_online():
            remote_bucket = self.s3_conn.get_bucket(self.s3_bucket)
            remote_key = Key(remote_bucket, self.s3_key)
            remote_key.set_contents_from_filename(self.local_path(), md5=(self.md5, self.b64, self.file_size))

        self.date_uploaded = now()

    def purge(self):
        if self.published:
            raise PurgingPublishedRecordError()

        assert len(self.delete()) ==  1

        if is_online():
            remote_bucket = self.s3_conn.get_bucket(self.s3_bucket)
            remote_key = Key(remote_bucket, self.s3_key)
            remote_bucket.delete_key(remote_key)

        swallow(OSError, lambda: os.unlink(self.local_path()))

    def download(self):
        """
        Download the file to the local cache
        """
        if not self.date_uploaded:
            raise RepoFileNotUploadedError()

        if os.path.exists(self.local_path()):
            return

        assert_online()

        remote_key = Key(self.s3_conn.get_bucket(self.s3_bucket), self.s3_key)
        remote_key.get_contents_to_filename(self.local_path())

        if self.md5:
            real_md5 = subprocess.check_output([ "md5", "-q", self.local_path() ])[:-1]
            if real_md5 != self.md5:
                raise RepoDownloadError()

    def open(self, mode='r'):
        """
        Returns a file pointer to the current file.
        """
        if mode == 'r' and self.date_uploaded:
            self.download()
        elif mode == 'w':
            mkdirp(os.path.dirname(self.local_path()))

        if self.s3_key.endswith(".gz"):
            return gzip.open(self.local_path(), mode)
        else:
            return open(self.local_path(), mode)

    def touch(self, contents = ""):
        """
        Ensures the repo file exists.
        """
        mkdirp(os.path.dirname(self.local_path()))
        with open(self.local_path(), 'a') as fp:
            if contents:
                fp.write(contents)
            fp.flush()

    def tag(self, *tags):
        def find_or_create(tag_id):
            found = list(self.Tag.find_by(tag_id = tag))
            if found:
                return found[0]
            return self.Tag(tag_id = tag).update()

        tags = [ find_or_create(tag).tag_no for tag in tags ]

        execute(self.conn, """
            INSERT INTO s3_repo_tags (
                file_no,
                tag_no,
                date_tagged
            ) SELECT
                file_no AS file_no,
                tag_no  AS tag_no,
                %(now)s AS date_tagged
            FROM (
                SELECT
                    %(file_no)s         AS file_no,
                    unnest(%(tag_nos)s) AS tag_no
                ) tmp_tags
            WHERE (file_no, tag_no) NOT IN (
                    SELECT file_no, tag_no
                    FROM s3_repo_tags
                )
        """,
            file_no = self.file_no,
            tag_nos = tags,
            now     = now(),
        )

    def untag(self, *tags):
        def find_or_create(tag_id):
            found = list(self.Tag.find_by(tag_id = tag))
            if found:
                return found[0]
            return self.Tag(tag_id = tag).update()

        tags = [ find_or_create(tag).tag_no for tag in tags ]

        execute(self.conn, """
            DELETE FROM s3_repo_tags
            WHERE file_no = %(file_no)s
                AND tag_no IN (%(tags)s)
        """,
            file_no = self.file_no,
            tags    = tuple(tags),
        )

    date_tags = {
        'hour'  : lambda x: 'hour='  + format_hour(x),
        'day'   : lambda x: 'day='   + format_day(x),
        'week'  : lambda x: 'week='  + format_week(x),
        'month' : lambda x: 'month=' + format_month(x),
    }

    tag_funcs = {
        'hour'  : [ 'hour',   'day',   'week',     'month' ],
        'day'   : [ 'day',    'week',  'month' ],
        'week'  : [ 'week',   ],
        'month' : [ 'month',  ],
    }

    def tag_date(self, period, type='hour'):
        period = coerce_date(period)
        for period_type in self.tag_funcs[type]:
            self.tag(self.date_tags[period_type](period))

    def __repr__(self):
        return "RepoFile({s3_path} ( {origin}:{local_path} )".format(
            s3_path    = self.s3_path(),
            local_path = self.local_path(),
            origin     = self.origin,
        )

class _Tag(DBTable):
    table_name = 's3_tags'
    key_field  = 'tag_no'
    db_conn    = None
    fields     = (
        'tag_no',
        'tag_id',
    )

    def __repr__(self):
        return "Tag({tag_no}, {tag_id})".format(**self.get_dict())

class _RepoFileTags(DBTable):
    table_name = 's3_repo_tags'
    db_conn    = None
    fields     = (
        'file_no',
        'tag_no',
        'date_tagged',
    )
