import os
import boto
import pyutil.pghelper
import pyutil.util

__all__ = [
    'db_conn',
    's3_conn',
    'S3RepoTable'
]

"""
{
    "backup.s3_bucket" : "some-bucket",
    "s3_access_key" : "abc",
    "s3_secret_key" : "def",
    "database" : {
        "host"     : "localhost",
        "port"     : 5432,
        "user"     : "s3repo",
        "password" : "s3repo",
        "database" : "s3repo"
    }
}
"""

def load_cfg():
    return pyutil.util.load_json_paths(
        os.environ.get('S3_REPO_CFG', None),
        '~/.s3repo.cfg',
    )

db_mgr = None
def db_conn(name = 'conn'):
    global db_mgr
    app_cfg = load_cfg()
    if not db_mgr:
        db_mgr = pyutil.pghelper.ConnMgr.default_from_info(**app_cfg['database'])

    return db_mgr.getconn(name)


_s3_conn = None
def s3_conn():
    global _s3_conn
    app_cfg = load_cfg()
    if not _s3_conn:
        _s3_conn = boto.connect_s3(
            app_cfg['s3_access_key'],
            app_cfg['s3_secret_key'],
        )

    return _s3_conn
