##############################################
# Database
##############################################

# DB
DROP DATABASE IF EXISTS publication_db;
CREATE DATABASE publication_db;
SHOW DATABASES;

# User
DROP USER IF EXISTS 'lbmcf'@'localhost';
CREATE USER 'lbmcf'@'localhost' IDENTIFIED BY 'P@55w0rd';
GRANT ALL PRIVILEGES ON publication_db.* To 'lbmcf'@'localhost';
FLUSH PRIVILEGES;

# Tables
USE publication_db;

# Drop
DROP TABLE IF EXISTS publication_today;
DROP TABLE IF EXISTS publication_updated;
DROP TABLE IF EXISTS publication_scopus;
DROP TABLE IF EXISTS publication_pubmed;
DROP TABLE IF EXISTS publication_log;

CREATE TABLE publication_log(
    id varchar(8) NOT NULL,
    process varchar(50),
    status varchar(10),
    message text,
    date_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_finish TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id, process));

CREATE TABLE publication_updated(
    id int auto_increment,
    scopus_id varchar(30),
    pubmed_id varchar(10),
    doi varchar(60),
    title varchar(1500),
    author varchar(1500),
    abstract varchar(8000),
    language varchar(50),
    cited_by int,
    publication_date varchar(20),
    publication_type varchar(200),
    document_type varchar(20),
    journal_name varchar(200),
    affiliation varchar(500),
    country varchar(40),
    repository varchar(40),
    publisher varchar(100),
    url varchar(80),
    INDEX index_doi_pub_updated(doi),
    date_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_update TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));

CREATE TABLE publication_today(
    id int auto_increment,
    scopus_id varchar(30),
    pubmed_id varchar(10),
    doi varchar(60),
    title varchar(1500),
    author varchar(1500),
    abstract varchar(8000),
    language varchar(50),
    cited_by int,
    publication_date varchar(20),
    publication_type varchar(200),
    document_type varchar(20),
    journal_name varchar(200),
    affiliation varchar(500),
    country varchar(40),
    repository varchar(40),
    date_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_update TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));

CREATE TABLE publication_scopus(
    id int auto_increment,
    scopus_id varchar(30),
    pubmed_id varchar(10),
    doi varchar(60),
    title varchar(1500),
    author varchar(1500),
    cited_by int,
    publication_date varchar(20),
    publication_type varchar(200),
    journal_name varchar(200),
    document_type varchar(20),
    document_code varchar(5),
    open_access varchar(5),
    affiliation varchar(500),
    date_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_update TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));

CREATE TABLE publication_pubmed(
    id int auto_increment,
    pubmed_id varchar(10),
    doi varchar(60),
    title varchar(1500),
    author varchar(1500),
    abstract varchar(8000),
    language varchar(50),
    publication_date varchar(20),
    publication_type varchar(200),
    journal_name varchar(200),
    country varchar(40),
    date_create TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_update TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id));
