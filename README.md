Repo
=======
Repo is a Python library for turning Amazon S3 into a distributed file repository.  Key features include:
- Immutable files designed for read after write consistency
- Flagging files as "published", which means that they should be available for consumption by the application
- Flagging files as "expired", which means they are no longer available for consumption
- Metadata verification of interactions with S3 (md5, file size)
- Eventual garbage collection of unpublished and expired files
- Local caches for accessed files to prevent re-downloading needed data
- Built in disaster recovery
- Simple backup and restore of database tables
- Date tagging files by hour/day/week/month
- Efficient finding files by tag combination (any, all, exclude)

The official repository is located at http://www.github.com/wizzat/s3repo.

#### Getting Started ####

Create a PostgreSQL database
Create a configuration file and put it in ~/.repo\_cfg.  It will look something like this:

    {
        "local_root" : "/mnt/repo",
        "default_s3_bucket" : "s3repo-test-bucket",
        "backup_s3_bucket"  : "s3repo-test-bucket",
        "s3_access_key" : "some-key",
        "s3_secret_key" : "some-secret",
        "database" : {
            "host"     : "localhost",
            "port"     : 5432,
            "user"     : "repo",
            "password" : "411B107B-A7ED-48AE-AE19-5BD427865041",
            "database" : "repo"
        }
    }

$ repo config --s3cmd
$ repo create

Typical usage inside of a Python application will look something like this:

    import repo

    repo = repo.Repo()

    rf = repo.add_file()
    rf.touch("this is some data that I want to write in my file")
    rf.publish()
    rf.tag('abc')
    rf.tag_date(now(), type='hour')
    repo.commit()

#### State Management ####
There are a variety of states that files can fall into:
- Unpublished: Either a new or transient file. Generally won't have md5 hash or file size, but should have origin. Will eventually be purged if it's not flagged as published.
- Published: A piece of data that is available for consumption by the application. Should never be purged unless flagged as 'expired'.
- Expired: A piece of data that was available for consumption but has been superceded or is no longer useful.

#### Tagging ####
Tagging
Date Tagging

#### Consistency and Disaster Recovery ####
Repo is based around the concept of immutable files to take advantage of read after write consistency available in certain S3 regions.  This means that if a file is available to download, it is also completely available.  It is highly recommended that you create your S3 buckets in these regions to take advantage.

There are three points of failure for disaster recovery purposes:
- The machines doing the processing
- The database of file states and tags
- The remote data store (S3)

Rules for handling errors in the machines doing processing:
- Data is only authoritative if it has been successfully uploaded to the data store
- Faulty or missing data can be re-downloaded from the data store.

Rules for handling database failure:
- Dumps of the repo and tags tables should be uploaded to the data store
- Restoration of the repo and tags tables should happen "blind" of knowledge in the database.
- Expired data should be kept in S3 for at least two back up cycles

Rules for handling data store failures:
- Find the latest backup where all published files exist and are valid
- Also check existing host machines for valid published data
- Cry

#### Todo ####

- Rename from s3repo to repo
- Allow backing by hpn-scp/scp/rsync
- Archival/Glacier integration
- Implement Console API
    - repo ctl --create --destroy --restore --config
    - repo add
    - repo tag [ --date ]
    - repo publish
    - repo upload
    - repo list [ pattern ]
    - repo find --any --all --exclude
- Examples
- File Permissions
- Installable into site-packages instead of via PYTHONPATH
- Better documentation
    - Library Reference
    - Man pages
    - Disaster Recovery Whitepaper
- Optionally purge s3 data not in database during restore operations
