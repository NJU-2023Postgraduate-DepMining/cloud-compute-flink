drop database if exists cloud;
create database cloud;
use cloud;
drop table if exists cloud.github_dependency_stats;
drop table if exists cloud.npm_dependency_stats;
drop table if exists cloud.max;

CREATE TABLE cloud.github_dependency_stats
(
    package_id          String,
    package_name        String,
    version             String,
    depended_time_stamp Int64,
    day                 String,
    month               String,
    year                String,
    depended_count      Int32
)ENGINE = MergeTree
primary key package_id;

CREATE TABLE cloud.npm_dependency_stats
(
    package_id          String,
    package_name        String,
    version             String,
    depended_time_stamp Int64,
    day                 String,
    month               String,
    year                String,
    depended_count      Int32
)ENGINE = MergeTree
primary key package_id;

CREATE TABLE cloud.max
(
    package_id          String,
    package_name        String,
    version             String,
    depended_time_stamp Int64,
    day                 String,
    month               String,
    year                String,
    depended_count      Int32
)ENGINE = MergeTree
primary key package_id;
