use role accountadmin;

create database if not exists flat_file;
use database flat_file;
create schema if not exists seed;
create schema if not exists raw;
create schema if not exists base;
create schema if not exists staging;

create database if not exists tech;
use database tech;
create schema if not exists intermediate;
create schema if not exists mart;

create database if not exists sap_p93;
use database sap_p93;
create schema if not exists raw;
create schema if not exists base;
create schema if not exists staging;

create database if not exists dbt_default;
use database dbt_default;
create schema if not exists analytics;