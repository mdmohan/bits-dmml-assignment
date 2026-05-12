-- Fact indexes

create index if not exists idx_fact_order_events_event_date
  on mart.fact_order_events (event_date);
create index if not exists idx_fact_order_events_order_event
  on mart.fact_order_events (order_id, event_type);
create index if not exists idx_fact_order_events_seller_date
  on mart.fact_order_events (seller_id, event_date);

create index if not exists idx_fact_payments_date_type
  on mart.fact_payments (event_date, payment_type);
create index if not exists idx_fact_payments_order_id
  on mart.fact_payments (order_id);
create index if not exists idx_fact_payments_status_date
  on mart.fact_payments (payment_status, event_date);

create index if not exists idx_fact_delivery_events_date_seller
  on mart.fact_delivery_events (event_date, seller_id);
create index if not exists idx_fact_delivery_events_order_id
  on mart.fact_delivery_events (order_id);
create index if not exists idx_fact_delivery_events_delay_hours
  on mart.fact_delivery_events (delivery_delay_hours);

create index if not exists idx_fact_reviews_date_score
  on mart.fact_reviews (event_date, review_score);
create index if not exists idx_fact_reviews_order_id
  on mart.fact_reviews (order_id);
create index if not exists idx_fact_reviews_seller_date
  on mart.fact_reviews (seller_id, event_date);

create index if not exists idx_fact_order_items_product_id
  on mart.fact_order_items (product_id);
create index if not exists idx_fact_order_items_seller_date
  on mart.fact_order_items (seller_id, event_date);
create index if not exists idx_fact_order_items_order_id
  on mart.fact_order_items (order_id);
