-- Dev helper: relax FK constraints on fact_order_events for incremental loading
-- Use this only in development/demo environments.

alter table mart.fact_order_events drop constraint if exists fk_foe_customer;
alter table mart.fact_order_events drop constraint if exists fk_foe_seller;

-- Recreate as DEFERRABLE to allow transaction-level deferred checks when needed.
alter table mart.fact_order_events
  add constraint fk_foe_customer
  foreign key (customer_id) references mart.dim_customer(customer_id)
  deferrable initially deferred;

alter table mart.fact_order_events
  add constraint fk_foe_seller
  foreign key (seller_id) references mart.dim_seller(seller_id)
  deferrable initially deferred;
