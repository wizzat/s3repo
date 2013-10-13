from pyutil.util import slurp
import boto, json, os

__all__ = [
    'list_bucket',
    'put_string',
]

config = json.loads(slurp(os.environ['S3CACHE_CONFIG']))
s3_conn = boto.connect_s3(
    config['s3_access_key'],
    config['s3_secret_key'],
)

def list_bucket(name, prefix=''):
    """
    Uses the generic S3 connection to list the contents of a bucket
    """
    global s3_conn
    return s3_conn.get_bucket(name).list(prefix)

def put_string(bucket, key, value):
    bucket = s3_conn.get_bucket(bucket)
    key = Key(bucket, key)
    key.set_contents(value)
