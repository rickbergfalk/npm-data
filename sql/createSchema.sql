
drop table if exists dependencies;
drop table if exists dev_dependencies;
drop table if exists keywords;
drop table if exists versions;
drop table if exists packages;

create table dependencies (
	package_name TEXT,
	dependency_name TEXT
);

create table dev_dependencies (
	package_name TEXT,
	dependency_name TEXT
);

create table keywords (
	package_name TEXT,
	keyword TEXT
);

create table versions (
    package_name text,
    version text,
    version_seq int,
    time TIMESTAMP,
    major BIGINT,
    minor BIGINT,
    patch BIGINT,
    prerelease text,
    publish_type text
);

create table packages (
    package_name text,
    latest_version text,
    readme text,
    created_date TIMESTAMP,
    modified_date TIMESTAMP,
    description text,
    repository text,
    license text,
    homepage text,
    author_name text,
    author_email text
);


create table rejected_packages (
    package_name text,
    rejected_reason text,
    doc jsonb
);

-- remove tables if they already exist for some reason
DROP TABLE IF EXISTS package_doc;

-- create a new table that stored packages in jsonb
CREATE TABLE package_doc (
    package_name TEXT PRIMARY KEY,
    doc JSONB NOT NULL
);