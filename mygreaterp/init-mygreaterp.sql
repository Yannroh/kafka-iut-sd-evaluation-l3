create table addresses
(
    id     varchar(36)  not null
        primary key,
    number int          null,
    street varchar(255) not null,
    city   varchar(255) not null,
    state  varchar(255) null
);

create table contacts
(
    id         varchar(36)  not null
        primary key,
    first_name varchar(255) null,
    last_name  varchar(255) not null
);

create table contacts_addresses
(
    id         varchar(36)  not null
        primary key,
    contact_id varchar(255) not null,
    address_id varchar(36)  not null,
    constraint contacts_addresses_addresses_id_fk
        foreign key (address_id) references addresses (id),
    constraint contacts_addresses_contacts_id_fk
        foreign key (contact_id) references contacts (id)
);

