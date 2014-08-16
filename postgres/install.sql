CREATE SCHEMA s3_repo;

CREATE TABLE s3_repo.hosts (
    host_id        SERIAL NOT NULL PRIMARY KEY,
    hostname       TEXT UNIQUE,
    max_cache_size BIGINT,
    active         BOOLEAN DEFAULT TRUE
);

CREATE TABLE s3_repo.s3_buckets (
    s3_bucket_id SERIAL NOT NULL PRIMARY KEY,
    s3_bucket    TEXT   NOT NULL UNIQUE
);

CREATE TABLE s3_repo.tags (
    tag_id SERIAL PRIMARY KEY,
    tag_name TEXT UNIQUE
);

CREATE TABLE s3_repo.paths (
    path_id      SERIAL PRIMARY KEY,
    local_path   TEXT
);

CREATE TABLE s3_repo.files (
    file_id          SERIAL PRIMARY KEY,
    s3_bucket_id     INTEGER NOT NULL REFERENCES s3_repo.s3_buckets(s3_bucket_id),
    path_id          INTEGER NOT NULL REFERENCES s3_repo.paths(path_id),
    origin           INTEGER NOT NULL REFERENCES s3_repo.hosts(host_id),
    s3_key           TEXT,
    md5              TEXT,
    b64              TEXT,
    guid             UUID,
    file_size        INTEGER,
    date_created     TIMESTAMP,
    date_uploaded    TIMESTAMP,
    date_published   TIMESTAMP,
    date_archived    TIMESTAMP,
    date_expired     TIMESTAMP,
    published        BOOLEAN DEFAULT FALSE,
    --
    UNIQUE (s3_bucket_id, s3_key)
);

CREATE INDEX ON s3_repo.files (path_id);
CREATE INDEX ON s3_repo.files (path_id, published) WHERE published = TRUE AND date_expired IS NULL;

CREATE TABLE s3_repo.file_tags (
    file_id     INTEGER NOT NULL REFERENCES s3_repo.files(file_id),
    tag_id      INTEGER NOT NULL REFERENCES s3_repo.tags(tag_id),
    date_tagged TIMESTAMP,
    --
    PRIMARY KEY (file_id, tag_id)
);

CREATE TABLE s3_repo.path_tags (
    path_id     INTEGER NOT NULL REFERENCES s3_repo.paths(path_id),
    tag_id      INTEGER NOT NULL REFERENCES s3_repo.tags(tag_id),
    date_tagged TIMESTAMP,
    --
    PRIMARY KEY (path_id, tag_id)
);

CREATE TABLE s3_repo.downloads (
    file_id        INTEGER NOT NULL REFERENCES s3_repo.files(file_id),
    host_id        INTEGER NOT NULL REFERENCES s3_repo.hosts(host_id),
    downloaded_utc TIMESTAMP NOT NULL,
    last_access    TIMESTAMP NOT NULL,
    --
    PRIMARY KEY (file_id, host_id)
);

CREATE OR REPLACE VIEW s3_repo.current_files AS
SELECT DISTINCT ON (path_id) *
FROM s3_repo.files
WHERE published = TRUE
    AND date_published IS NOT NULL
    AND date_expired IS NULL
ORDER BY path_id, date_published DESC;

CREATE OR REPLACE VIEW s3_repo.current_file_tags AS
SELECT s3_repo.current_files.*, s3_repo.path_tags.tag_id, s3_repo.tags.tag_name
FROM s3_repo.current_files
    INNER JOIN s3_repo.path_tags USING (path_id)
    INNER JOIN s3_repo.tags USING (tag_id)
UNION ALL
SELECT s3_repo.current_files.*, s3_repo.file_tags.tag_id, s3_repo.tags.tag_name
FROM s3_repo.current_files
    INNER JOIN s3_repo.file_tags USING (file_id)
    INNER JOIN s3_repo.tags USING (tag_id)
;

CREATE OR REPLACE VIEW s3_repo.all_file_tags AS
SELECT s3_repo.files.*, s3_repo.path_tags.tag_id, s3_repo.tags.tag_name
FROM s3_repo.files
    INNER JOIN s3_repo.path_tags USING (path_id)
    INNER JOIN s3_repo.tags USING (tag_id)
UNION ALL
SELECT s3_repo.files.*, s3_repo.file_tags.tag_id, s3_repo.tags.tag_name
FROM s3_repo.files
    INNER JOIN s3_repo.file_tags USING (file_id)
    INNER JOIN s3_repo.tags USING (tag_id)
;

CREATE OR REPLACE VIEW s3_repo.host_cache_stats AS
SELECT
    host_id                                   AS host_id,
    total_size                                AS total_size,
    s3_repo.hosts.max_cache_size              AS max_cache_size,
    total_size - s3_repo.hosts.max_cache_size AS overflow_bytes
FROM (
    SELECT
        host_id           AS host_id,
        sum(rf.file_size) AS total_size
    FROM s3_repo.files rf
        INNER JOIN s3_repo.downloads
            USING (file_id)
    GROUP BY host_id
    ) x INNER JOIN s3_repo.hosts
            USING (host_id)
;


CREATE OR REPLACE VIEW s3_repo.deletable_files AS
SELECT rf.*
FROM s3_repo.files rf
    LEFT OUTER JOIN (
        SELECT *
        FROM s3_repo.downloads dl
            INNER JOIN s3_repo.hosts h
                USING (host_id)
        WHERE h.active
    ) dl USING (file_id)
WHERE dl.host_id IS NULL
    AND NOT rf.published
;
