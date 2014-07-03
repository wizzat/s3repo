DROP VIEW s3_repo.current_files;
DROP VIEW s3_repo.current_file_tags;
DROP VIEW s3_repo.all_file_tags;

DROP TABLE s3_repo.hosts, s3_repo.tags, s3_repo.files, s3_repo.file_tags, s3_repo.path_tags, s3_repo.downloads;

DROP SCHEMA s3_repo;

COMMIT;
