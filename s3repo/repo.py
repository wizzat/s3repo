import os, uuid
import s3repo.common
import s3repo.host
import s3repo.file
from s3repo.exceptions import *
from pyutil.dateutil import *
from pyutil.util import set_defaults

class S3Repo(object):
    config = s3repo.common.load_cfg()
    conn = s3repo.common.db_conn()

    commit = conn.commit
    rollback = conn.rollback

    @classmethod
    def add_file(cls, path, **kwargs):
        local_path = s3repo.file.LocalPath.find_or_create(path)
        s3_bucket  = s3repo.file.S3Bucket.find_or_create(kwargs.pop('s3_bucket', cls.config['s3.default_bucket']))

        kwargs = set_defaults(kwargs,
            s3_key       = os.path.join(path, str(to_epoch(now()))),
            guid         = str(uuid.uuid4()),
            path_id      = local_path.path_id,
            origin       = s3repo.host.RepoHost.current_host_id(),
            date_created = now(),
        )

        rf = s3repo.file.RepoFile.find_or_create(s3_bucket.s3_bucket_id, kwargs['s3_key'], **kwargs)

        if rf.guid != kwargs['guid']:
            raise RepoConcurrentInsertionError((path, s3_bucket.s3_bucket, kwargs['s3_key'], kwargs['guid']))

        return rf

    @classmethod
    def get_file(cls, path):
        local_path = s3repo.file.LocalPath.find(path)
        if not local_path:
            return None

        return local_path.find_current()

    @classmethod
    def backup_table(cls, conn, table_obj):
        local_path = os.path.join(
            cls.config['backup.local.path'],
            table_obj.table_name,
        )

        backup_file = cls.add_file(local_path + '.gz', s3_bucket = cls.config['backup.s3_bucket'])
        with backup_file.open('w') as fp:
            conn.cursor().copy_to(fp, table_obj.table_name, columns = table_obj.fields)

        return backup_file

    @classmethod
    def restore_table(cls, conn, table_obj):
        local_path = os.path.join(
            cls.config['backup.local.path'],
            table_obj.table_name,
        )

        backup_bucket = s3repo.common.s3_conn().get_bucket(cls.config['backup.s3_bucket'])
        remote_backup_files = backup_bucket.list(local_path)

        try:
            last_backup = sorted(remote_backup_files, key=lambda x: x.name)[-1]
        except IndexError:
            raise RepoNoBackupsError()

        fp = tempfile.NamedTemporaryFile()
        last_backup.get_contents_to_filename(fp.name)

        with gzip.open(fp.name, 'r') as fp:
            conn.cursor().copy_from(fp, table_obj.table_name, columns = table_obj.fields)


    @classmethod
    def backup_db(cls, conn):
        """
        Creates a backup of the S3 Cache and uploads it to config['backup_s3_bucket']/s3repo_backups
        Backup name will be: "YYYY-MM-DD_HH:24:MI:SS.sql.gz"
        Ensures that no more than config['num_backups'] exist.
        """

        raise NotImplemented()
        backup_files = []
        for table_obj in []:
            backup_files.append(cls.backup_table(conn, table_obj))

        for backup_file in backup_files:
            backup_file.publish()

    @classmethod
    def restore_db(cls):
        """
        Queries config['backup_s3_bucket']/s3repo_backups and restores the latest backup.
        """
        for table_obj in []:
            cls.restore_table(conn, table_obj)
        raise NotImplemented()

    @classmethod
    def cleanup_unpublished_files(cls):
        """
        Purges local files which have not been published
        """
        unpublished_files = s3repo.file.RepoFile.find_by(
            origin         = s3repo.host.RepoHost.current_host_id(),
            published      = False,
            date_published = None,
        )

        for rf in unpublished_files:
            if rf.date_created < now() - hours(int(cls.config['unpublished.cleanup.hours'])):
                rf.purge()

    @classmethod
    def cleanup_local_disk(cls):
        """
        Recursively examines config['local_root'] and unlinks files which have been accessed more than config['local_atime_limit'] minutes ago.
        """
        raise NotImplemented()

    @classmethod
    def find_tagged(cls, any = None, all = None, exclude = None, published = True):
        """
        """
        all_tags     = s3repo.tag.Tag.find_tag_ids(all or [])
        any_tags     = s3repo.tag.Tag.find_tag_ids(any or [])
        exclude_tags = s3repo.tag.Tag.find_tag_ids(exclude or [])
        hint_tags    = all_tags + any_tags + exclude_tags

        if exclude:
            if not all_tags and not any_tags:
                raise RepoAPIError("exclude requires any or all")

        where_filters  = [ 'true' ]
        having_filters = [ 'true' ]

        if all_tags:
            having_filters.append("sum(CASE WHEN tag_id IN %(all_tags)s THEN 1 ELSE 0 END) = %(num_all_tags)s")

        if any_tags:
            having_filters.append("sum(case when tag_id in %(any_tags)s then 1 else 0 end) >= 1")

        if exclude_tags:
            having_filters.append("sum(case when tag_id in %(exclude_tags)s then 1 else 0 end) = 0")

        if hint_tags:
            where_filters.append("tag_id IN %(hint_tags)s")

        query = """
            SELECT *
            FROM s3_repo.files
            WHERE file_id in (
                    SELECT file_id
                    FROM {source_view}
                    WHERE {where_filter}
                    GROUP BY file_id
                    HAVING {having_filter}
                )
        """.format(
            source_view   = 's3_repo.current_file_tags' if published else 's3_repo.all_file_tags',
            where_filter  = '\n    AND '.join(where_filters),
            having_filter = '\n    AND '.join(having_filters),
        )

        return s3repo.file.RepoFile.find_by_sql(query,
            num_all_tags = len(all_tags),
            all_tags     = tuple(all_tags),
            any_tags     = tuple(any_tags),
            exclude_tags = tuple(exclude_tags),
            hint_tags    = tuple(hint_tags),
        )
