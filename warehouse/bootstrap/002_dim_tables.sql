-- Dimension tables

create table if not exists mart.dim_date (
  date_key int primary key,
  full_date date unique not null,
  day_of_week int,
  day_name text,
  day_of_month int,
  week_of_year int,
  month int,
  month_name text,
  quarter int,
  year int,
  is_weekend boolean,
  is_month_start boolean,
  is_month_end boolean
);

create table if not exists mart.dim_customer (
  customer_key bigserial primary key,
  customer_id text unique not null,
  customer_unique_id text,
  customer_city text,
  customer_state text,
  customer_zip_prefix text,
  created_at timestamp default current_timestamp,
  updated_at timestamp
);

create index if not exists idx_dim_customer_state_city
  on mart.dim_customer (customer_state, customer_city);

create table if not exists mart.dim_seller (
  seller_key bigserial primary key,
  seller_id text unique not null,
  seller_city text,
  seller_state text,
  seller_zip_prefix text,
  created_at timestamp default current_timestamp,
  updated_at timestamp
);

create index if not exists idx_dim_seller_state_city
  on mart.dim_seller (seller_state, seller_city);

create table if not exists mart.dim_product (
  product_key bigserial primary key,
  product_id text unique not null,
  product_category_name text,
  product_name_lenght int,
  product_description_lenght int,
  product_photos_qty int,
  product_weight_g int,
  product_length_cm int,
  product_height_cm int,
  product_width_cm int,
  created_at timestamp default current_timestamp,
  updated_at timestamp
);

create index if not exists idx_dim_product_category
  on mart.dim_product (product_category_name);
