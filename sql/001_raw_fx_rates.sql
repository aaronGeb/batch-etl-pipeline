create schema if not exists raw;

create table if not exists raw.fx_rates (
    rate_date    date           not null,
    base         text           not null,
    currency     text           not null,
    rate         numeric(18, 8) not null,
    source_file  text           not null,
    loaded_at    timestamptz    not null default now(),
    primary key (rate_date, base, currency)
);
