-- Fact tables

create table if not exists mart.fact_order_events (
  event_id text primary key,
  order_id text not null,
  event_type text not null,
  event_ts timestamp not null,
  event_date date not null,
  customer_id text,
  seller_id text,
  order_status text,
  purchase_ts timestamp,
  approved_ts timestamp,
  estimated_delivery_ts timestamp,
  source_system text not null,
  schema_version text not null,
  ingestion_ts timestamp not null
);

create table if not exists mart.fact_payments (
  event_id text primary key,
  order_id text not null,
  payment_sequential int not null,
  payment_type text not null,
  payment_installments int,
  payment_value numeric(18,2) not null,
  payment_status text not null,
  event_ts timestamp not null,
  event_date date not null,
  customer_id text,
  source_system text not null,
  schema_version text not null,
  ingestion_ts timestamp not null,
  check (payment_value >= 0)
);

create table if not exists mart.fact_delivery_events (
  event_id text primary key,
  order_id text not null,
  event_type text not null,
  carrier_status text,
  shipped_ts timestamp,
  delivered_customer_ts timestamp,
  delivery_delay_hours numeric(10,2),
  event_ts timestamp not null,
  event_date date not null,
  seller_id text,
  customer_id text,
  source_system text not null,
  schema_version text not null,
  ingestion_ts timestamp not null
);

create table if not exists mart.fact_reviews (
  event_id text primary key,
  review_id text not null,
  order_id text not null,
  review_score int not null check (review_score between 1 and 5),
  review_created_ts timestamp not null,
  review_answer_ts timestamp,
  review_response_hours numeric(10,2),
  review_comment_title text,
  event_ts timestamp not null,
  event_date date not null,
  customer_id text,
  seller_id text,
  source_system text not null,
  schema_version text not null,
  ingestion_ts timestamp not null
);

create table if not exists mart.fact_order_items (
  order_id text not null,
  order_item_id int not null,
  product_id text not null,
  seller_id text not null,
  shipping_limit_date timestamp,
  price numeric(18,2),
  freight_value numeric(18,2),
  event_date date,
  primary key (order_id, order_item_id),
  check (coalesce(price, 0) >= 0),
  check (coalesce(freight_value, 0) >= 0)
);

create table if not exists mart.fact_data_quality (
  run_id text not null,
  dataset_name text not null,
  run_ts timestamp not null,
  rows_in bigint,
  rows_out bigint,
  null_violations bigint,
  duplicate_violations bigint,
  schema_violations bigint,
  status text,
  primary key (run_id, dataset_name)
);
