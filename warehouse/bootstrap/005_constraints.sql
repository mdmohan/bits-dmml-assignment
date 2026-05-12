-- Foreign keys (business-key based joins)

alter table mart.fact_order_events
  add constraint fk_foe_customer
  foreign key (customer_id) references mart.dim_customer(customer_id);

alter table mart.fact_order_events
  add constraint fk_foe_seller
  foreign key (seller_id) references mart.dim_seller(seller_id);

alter table mart.fact_payments
  add constraint fk_fp_customer
  foreign key (customer_id) references mart.dim_customer(customer_id);

alter table mart.fact_delivery_events
  add constraint fk_fde_customer
  foreign key (customer_id) references mart.dim_customer(customer_id);

alter table mart.fact_delivery_events
  add constraint fk_fde_seller
  foreign key (seller_id) references mart.dim_seller(seller_id);

alter table mart.fact_reviews
  add constraint fk_fr_customer
  foreign key (customer_id) references mart.dim_customer(customer_id);

alter table mart.fact_reviews
  add constraint fk_fr_seller
  foreign key (seller_id) references mart.dim_seller(seller_id);

alter table mart.fact_order_items
  add constraint fk_foi_product
  foreign key (product_id) references mart.dim_product(product_id);

alter table mart.fact_order_items
  add constraint fk_foi_seller
  foreign key (seller_id) references mart.dim_seller(seller_id);
