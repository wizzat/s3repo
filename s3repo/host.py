import socket
import s3repo.common
import pyutil.pghelper

class RepoHost(pyutil.pghelper.DBTable):
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
    ]

    @classmethod
    def current_host(cls):
        return cls.find_or_create(socket.gethostname())

    @classmethod
    def current_host_id(cls):
        return cls.current_host().host_id

    def decomm(self):
        for obj in RepoFileDownload.find_by(host_id = self.host_id):
            obj.delete()
        self.delete()


class RepoFileDownload(pyutil.pghelper.DBTable):
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
    ]
