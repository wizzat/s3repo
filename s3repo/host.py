import socket
import s3repo.common
import pyutil.pghelper
import pyutil.dbtable
from pyutil.decorators import *
from pyutil.dateutil import *

class RepoHost(pyutil.dbtable.DBTable):
    table_name = 's3_repo.hosts'
    memoize    = True
    conn       = s3repo.common.db_conn()

    id_field   = 'host_id'
    key_fields = [
        'hostname'
    ]

    fields = [
        'host_id',
        'hostname',
        'max_cache_size',
    ]

    @classmethod
    def current_host(cls):
        return cls.find_or_create(socket.gethostname())

    @classmethod
    def current_host_id(cls):
        return cls.current_host().host_id

    def decomm(self):
        RepoFileDownload.purge_host(self.host_id)
        self.delete()


class RepoFileDownload(pyutil.dbtable.DBTable):
    table_name = 's3_repo.downloads'
    conn       = s3repo.common.db_conn()

    key_fields = [
        'file_id',
        'host_id',
    ]

    fields = [
        'file_id',
        'host_id',
        'downloaded_utc',
        'last_access',
    ]

    @classmethod
    def update_access_time(cls, rf):
        rf = cls.find_or_create(rf.file_id, RepoHost.current_host_id(),
            last_access    = now(),
        )
        rf.last_access = now()
        rf.update()

    @classmethod
    def flag_download(cls, rf):
        cls.find_or_create(rf.file_id, RepoHost.current_host_id(),
            downloaded_utc = now(),
            last_access    = now(),
        )

    @classmethod
    def remove_download(cls, rf):
        rf = cls.find_by_key(rf.file_id, RepoHost.current_host_id())
        if rf:
            rf.delete()

    @classmethod
    def purge_host(cls, host_id):
        execute(cls.conn, """
            DELETE FROM s3_repo.downloads
            WHERE host_id = %(host_id)s
        """, host_id = host_id)

    @memoize_property
    def repo_file(self):
        return s3repo.file.RepoFile.find_by_id(self.file_id)
