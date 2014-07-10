import s3repo.common
import pyutil.pghelper
from pyutil.pghelper import fetch_results, execute
from pyutil.dateutil import *

class Tag(pyutil.pghelper.DBTable):
    table_name = 's3_repo.tags'
    memoize    = True
    conn       = s3repo.common.db_conn()

    id_field   = 'tag_id'
    key_fields = [
        'tag_name',
    ]

    fields     = [
        'tag_id',
        'tag_name',
    ]

    def __repr__(self):
        return "Tag({tag_id}, {tag_name})".format(**self.get_dict())

    @classmethod
    def find_tag_ids(cls, tag_names):
        assert isinstance(tag_names, (list, tuple))
        existing_tags = cls.find_by(
            tag_name = tuple(tag_names),
        )

        return [ x.tag_id for x in existing_tags ]

    @classmethod
    def find_or_create_tag_ids(cls, tag_names):
        assert isinstance(tag_names, (list, tuple))

        execute(cls.conn, """
            INSERT INTO s3_repo.tags (
                tag_name
            )
            SELECT tag_name
            FROM (VALUES {}) AS new_tags(tag_name)
            WHERE NOT EXISTS (
                SELECT 1
                FROM s3_repo.tags
                WHERE s3_repo.tags.tag_name = new_tags.tag_name
            )
        """.format(', '.join("('{}')".format(x) for x in tag_names)))

        return cls.find_tag_ids(tag_names)


class RepoFileTag(pyutil.pghelper.DBTable):
    table_name = 's3_repo.file_tags'
    conn = s3repo.common.db_conn()

    key_fields = [
        'file_id',
        'tag_id',
    ]

    fields = [
        'file_id',
        'tag_id',
        'date_tagged',
    ]

    @classmethod
    def tag_file(cls, file_id, *tag_names):
        tag_ids = Tag.find_or_create_tag_ids(tag_names)

        query = """
            INSERT INTO s3_repo.file_tags (
                file_id,
                tag_id,
                date_tagged
            )
            SELECT
                %(file_id)s AS file_id,
                tag_id      AS tag_id,
                %(now)s     AS date_tagged
            FROM (VALUES {}) AS new_tags(tag_id)
            WHERE NOT EXISTS (
                SELECT 1
                FROM s3_repo.file_tags
                WHERE s3_repo.file_tags.file_id = %(file_id)s
                    AND s3_repo.file_tags.tag_id = new_tags.tag_id
            )
        """.format(', '.join("({})".format(x) for x in tag_ids))

        execute(cls.conn, query,
            file_id = file_id,
            now     = now(),
        )

    @classmethod
    def untag_file(cls, file_id, *tag_names):
        tag_ids = Tag.find_tag_ids(tag_names)

        if tag_ids:
            execute(cls.conn, """
                DELETE FROM s3_repo.file_tags
                WHERE file_id = %(file_id)s
                    AND tag_id in %(tag_ids)s
            """,
                file_id = file_id,
                tag_ids = tuple(tag_ids),
            )


class RepoPathTag(pyutil.pghelper.DBTable):
    table_name = 's3_repo.path_tags'
    conn       = s3repo.common.db_conn()

    key_fields = [
        'path_id',
        'tag_id',
    ]

    fields = [
        'path_id',
        'tag_id',
        'date_tagged',
    ]

    @classmethod
    def tag_path(cls, path_id, *tag_names):
        tag_ids = Tag.find_or_create_tag_ids(tag_names)

        if tag_names:
            execute(cls.conn, """
                INSERT INTO s3_repo.path_tags (
                    path_id,
                    tag_id,
                    date_tagged
                )
                SELECT
                    %(path_id)s AS path_id,
                    tag_id      AS tag_id,
                    %(now)s     AS date_tagged
                FROM s3_repo.path_tags
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM s3_repo.file_tags
                    WHERE path_id = %(path_id)s
                        and tag_id in %(tag_ids)s
                )
            """,
                path_id = path_id,
                tag_ids = tuple(tag_ids),
                now     = now(),
            )

    @classmethod
    def untag_path(cls, path_id, *tag_names):
        tag_ids = Tag.find_tag_ids(tag_names)

        if tag_ids:
            execute(cls.conn, """
                DELETE FROM s3_repo.path_tags
                WHERE path_id = %(path_id)s
                    AND tag_id in %(tag_ids)s
            """,
                path_id = path_id,
                tag_ids = tuple(tag_ids),
            )
