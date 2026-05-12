-- Seed optional dimensions (safe upserts)

insert into mart.dim_payment_type (payment_type, is_digital, is_installment_supported)
values
  ('credit_card', true, true),
  ('debit_card', true, false),
  ('voucher', true, false),
  ('boleto', false, false)
on conflict (payment_type) do nothing;

insert into mart.dim_event_type (event_type, domain, is_terminal_state)
values
  ('order_created', 'order', false),
  ('order_approved', 'order', false),
  ('order_canceled', 'order', true),
  ('order_delivered', 'delivery', true),
  ('payment_approved', 'payment', false),
  ('payment_failed', 'payment', true),
  ('review_submitted', 'review', true)
on conflict (event_type) do nothing;
