S3Repo
=======
S3Repo is a Python library for turning Amazon S3 into a distributed file repository.  Key features include:
- Flagging files as "published", which means that they should be available for consumption by the application
- Flagging files as "expired", which means they are no longer available for consumption by the application
- Metadata verification of interactions with S3 (md5, file size)
- Eventual garbage collection of unpublished and expired files
- Simple backup and restore
- Local caches for accessed files to prevent re-downloading needed data.

The official repository is located at http://www.github.com/wizzat/s3repo.

Wish List:
- File Permissions

#### Getting Started ####

Create a PostgreSQL database
Create a S3Repo Configuration file.  It will look something like this (we'll go into the options later):
    {
        "local_root" : "/mnt/s3repo",
        "default_s3_bucket" : "s3repo-test-bucket",
        "backup_s3_bucket"  : "s3repo-test-bucket",
        "s3_access_key" : "some-key",
        "s3_secret_key" : "some-secret",
        "database" : {
            "host"     : "localhost",
            "port"     : 5432,
            "user"     : "pyutil",
            "password" : "pyutil",
            "database" : "pyutil_testdb"
        }
    }

Set the environment variable S3REPO\_CONFIG to point at the previously created file
Set PYTHONPATH to include the s3repo checkout

$ export PYTHONPATH=...
$ export S3REPO\_CONFIG=...
$ python
    >>> import s3repo
    >>> s3repo.S3Repo().create_repository()

#### Usage ####
Typical usage of S3Repo will look something like this:

    import s3repo

    repo =s3repo.S3Repo()

    rf = self.repo.add_file(file_type = 'story_data')
    rf.touch("this is some data that I want to write in my file")
    rf.publish()
    repo.commit()

#### State Management ####
There are a variety of states that files can fall into:
- Files which have not been flagged as published yet tend to be either new or transient files.  They will probably not hve md5 hashes or file sizes associated with them, and will be eventually purged if they're not ever published.
- Files which have been published represent pieces of data that you want to keep around as being currently useful.  They will not ever be purged unless you later flag them as 'expired'.
- Files which have been expired represent pieces of data that were previously useful, but have been superceded for one reason or another.

#### S3 and Consistency ####
First, it's worth understanding the difference between _eventual consistency_ and _read after write consistency_.  S3Repo is based around the concept of immutable files, which allows us to take advantage of _read after write consistency_ in certain S3 regions.  The short take away is that if the file is present to be downloaded, it is also complete.

It is highly recommended that you create your S3 buckets in these region to take advantage.

#### Disaster Recovery ####
There are four possible disaster recovery scenarios, presented in order of likelihood.

##### Database Failure #####
If your database is hosted on EC2 (and it probably should be), this is far and away the most likely failure case.
Fetch latest backup from S3, restore backup.

##### Missing S3 Keys and Incomplete/Incorrect Uploads #####
This can happen despite S3's tremendous uptime.  Incomplete or partial uploads should be caught immediately.  Missing S3 keys may or may not be recoverable.

Partial failure (missing keys, incorrect data): attempt to re-upload data from the original host, or rollback repo to specific backup before key went missing

##### Missing S3 Buckets #####
The most likely case when this happens is someone accidentally the system.
Complete failure: upload s3 buckets from Glaicer or an external data store.  Restore latest backup.

##### Simultaneous Failure #####
Welp.  I'm sure there's something clever to do here.

#### File States ####

##### Unpublished #####

##### Published #####
Published files are active and available for use.

##### Expired #####
Expired files have been deleted and are no longer necessary.  They are still in S3.

The goal of this project is to use S3 as a stateful file repository.  As such, it supports state (Unpublished, Published, Expired, Purged) for each file as well as metadata collection.  Files are cached locally
The goal of this project is to use S3 as a file repository with a distributed local cache.
This project is in development.  Don't use it.

Deployment notes:
- Set the environment variable S3CACHE\_CONFIG to point at a valid json configuration file.

An example S3CACHE\_CONFIG
    {
        "local_root" : "/mnt/s3repo",
        "default_s3_bucket" : "s3repo-test-bucket",
        "backup_s3_bucket"  : "s3repo-test-bucket",
        "s3_access_key" : "some-key",
        "s3_secret_key" : "some-secret",
        "database" : {
            "host"     : "localhost",
            "port"     : 5432,
            "user"     : "pyutil",
            "password" : "pyutil",
            "database" : "pyutil_testdb"
        }
    }

Desired Features:
- Documentation
    - Python API
    - Console commands
    - Disaster Recovery whitepaper
- Add files to repo
    - Create new local files [ Done ]
    - Copy local files[ Done ]
    - Move local files[ Done ]
- Store file metadaata
    - Origin host [ Done ]
    - File size [ Done ]
    - MD5 upload verification [ Done ]
- File State Management
    - States: Unpublished/Published/Expired [ Done ]
    - States: Local/Uploaded [ Done ]
    - States: Fresh/Archived
- Row level repo locks to signal processing [ Done ]
- Upload files to S3
    - Check md5 hash with S3 on upload
- Disaster Recovery
    - Dump DB state into S3 via copy files
    - Restore DB state from S3 via copy files
    - Purge S3 of data that is not in database
- Cost cutting via Glacier integration
