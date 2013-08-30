S3Cache
=======
The goal of this project is to use S3 as a file repository with a distributed local cache.
This project is in development.  Don't use it.

Deployment notes:
- Set the environment variable S3CACHE\_CONFIG to point at a valid json configuration file.
- The target database must have the hstore extension installed.  (echo "create extension hstore;commit;" | sudo -u postgres psql -d $TARGET\_DATABASE)

Desired 0.1 Features:
- Create file repository in S3
- Add file to repository
- Flag files as published or expired
- Find published files for file type and/or date
- Lock a row for processing
- Use HStore for semistructured data (like date, etc)
- md5 hash checking with S3

Desired 1.0 Features:
- Dump DB state into S3
- Restore DB state from S3
- Remove data from S3 that is unpublished and backed up

Desired 1.1 Features:
- Cost cutting via Glacier integration
