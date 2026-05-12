-- Optional governance dimensions

create table if not exists mart.dim_payment_type (
  payment_type_key smallserial primary key,
  payment_type text unique not null,
  is_digital boolean,
  is_installment_supported boolean
);

create table if not exists mart.dim_event_type (
  event_type_key smallserial primary key,
  event_type text unique not null,
  domain text,
  is_terminal_state boolean
);
