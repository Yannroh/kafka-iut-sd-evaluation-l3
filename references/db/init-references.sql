create database references;

use references;

create table customers_ref
(
    eurynome_id     int  not null ,
    mygreaterp_id varchar(36) not null,
    primary key(eurynome_id,mygreaterp_id) 
);

