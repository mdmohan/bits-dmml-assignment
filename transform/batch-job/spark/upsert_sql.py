UPSERT_SQL = {
    "mart.dim_customer": """
        insert into mart.dim_customer (
            customer_id, customer_unique_id, customer_city, customer_state,
            customer_zip_prefix, created_at, updated_at
        )
        select
            customer_id, customer_unique_id, customer_city, customer_state,
            customer_zip_prefix, created_at, updated_at
        from mart.stg_dim_customer
        on conflict (customer_id)
        do update set
            customer_unique_id = excluded.customer_unique_id,
            customer_city = excluded.customer_city,
            customer_state = excluded.customer_state,
            customer_zip_prefix = excluded.customer_zip_prefix,
            updated_at = current_timestamp;
        truncate table mart.stg_dim_customer;
    """,
    "mart.dim_seller": """
        insert into mart.dim_seller (
            seller_id, seller_city, seller_state, seller_zip_prefix, created_at, updated_at
        )
        select
            seller_id, seller_city, seller_state, seller_zip_prefix, created_at, updated_at
        from mart.stg_dim_seller
        on conflict (seller_id)
        do update set
            seller_city = excluded.seller_city,
            seller_state = excluded.seller_state,
            seller_zip_prefix = excluded.seller_zip_prefix,
            updated_at = current_timestamp;
        truncate table mart.stg_dim_seller;
    """,
    "mart.dim_product": """
        insert into mart.dim_product (
            product_id, product_category_name, product_name_lenght, product_description_lenght,
            product_photos_qty, product_weight_g, product_length_cm, product_height_cm,
            product_width_cm, created_at, updated_at
        )
        select
            product_id, product_category_name, product_name_lenght, product_description_lenght,
            product_photos_qty, product_weight_g, product_length_cm, product_height_cm,
            product_width_cm, created_at, updated_at
        from mart.stg_dim_product
        on conflict (product_id)
        do update set
            product_category_name = excluded.product_category_name,
            product_name_lenght = excluded.product_name_lenght,
            product_description_lenght = excluded.product_description_lenght,
            product_photos_qty = excluded.product_photos_qty,
            product_weight_g = excluded.product_weight_g,
            product_length_cm = excluded.product_length_cm,
            product_height_cm = excluded.product_height_cm,
            product_width_cm = excluded.product_width_cm,
            updated_at = current_timestamp;
        truncate table mart.stg_dim_product;
    """,
    "mart.fact_order_events": """
        insert into mart.fact_order_events (
            event_id, order_id, event_type, event_ts, event_date, customer_id, seller_id,
            order_status, purchase_ts, approved_ts, estimated_delivery_ts,
            source_system, schema_version, ingestion_ts
        )
        select
            event_id, order_id, event_type, event_ts, event_date, customer_id, seller_id,
            order_status, purchase_ts, approved_ts, estimated_delivery_ts,
            source_system, schema_version, ingestion_ts
        from mart.stg_fact_order_events
        on conflict (event_id)
        do update set
            order_id = excluded.order_id,
            event_type = excluded.event_type,
            event_ts = excluded.event_ts,
            event_date = excluded.event_date,
            customer_id = excluded.customer_id,
            seller_id = excluded.seller_id,
            order_status = excluded.order_status,
            purchase_ts = excluded.purchase_ts,
            approved_ts = excluded.approved_ts,
            estimated_delivery_ts = excluded.estimated_delivery_ts,
            source_system = excluded.source_system,
            schema_version = excluded.schema_version,
            ingestion_ts = excluded.ingestion_ts;
        truncate table mart.stg_fact_order_events;
    """,
    "mart.fact_payments": """
        insert into mart.fact_payments (
            event_id, order_id, payment_sequential, payment_type, payment_installments,
            payment_value, payment_status, event_ts, event_date, customer_id,
            source_system, schema_version, ingestion_ts
        )
        select
            event_id, order_id, payment_sequential, payment_type, payment_installments,
            payment_value, payment_status, event_ts, event_date, customer_id,
            source_system, schema_version, ingestion_ts
        from mart.stg_fact_payments
        on conflict (event_id)
        do update set
            order_id = excluded.order_id,
            payment_sequential = excluded.payment_sequential,
            payment_type = excluded.payment_type,
            payment_installments = excluded.payment_installments,
            payment_value = excluded.payment_value,
            payment_status = excluded.payment_status,
            event_ts = excluded.event_ts,
            event_date = excluded.event_date,
            customer_id = excluded.customer_id,
            source_system = excluded.source_system,
            schema_version = excluded.schema_version,
            ingestion_ts = excluded.ingestion_ts;
        truncate table mart.stg_fact_payments;
    """,
    "mart.fact_delivery_events": """
        insert into mart.fact_delivery_events (
            event_id, order_id, event_type, carrier_status, shipped_ts, delivered_customer_ts,
            delivery_delay_hours, event_ts, event_date, seller_id, customer_id,
            source_system, schema_version, ingestion_ts
        )
        select
            event_id, order_id, event_type, carrier_status, shipped_ts, delivered_customer_ts,
            delivery_delay_hours, event_ts, event_date, seller_id, customer_id,
            source_system, schema_version, ingestion_ts
        from mart.stg_fact_delivery_events
        on conflict (event_id)
        do update set
            order_id = excluded.order_id,
            event_type = excluded.event_type,
            carrier_status = excluded.carrier_status,
            shipped_ts = excluded.shipped_ts,
            delivered_customer_ts = excluded.delivered_customer_ts,
            delivery_delay_hours = excluded.delivery_delay_hours,
            event_ts = excluded.event_ts,
            event_date = excluded.event_date,
            seller_id = excluded.seller_id,
            customer_id = excluded.customer_id,
            source_system = excluded.source_system,
            schema_version = excluded.schema_version,
            ingestion_ts = excluded.ingestion_ts;
        truncate table mart.stg_fact_delivery_events;
    """,
    "mart.fact_reviews": """
        insert into mart.fact_reviews (
            event_id, review_id, order_id, review_score, review_created_ts, review_answer_ts,
            review_response_hours, review_comment_title, event_ts, event_date, customer_id,
            seller_id, source_system, schema_version, ingestion_ts
        )
        select
            event_id, review_id, order_id, review_score, review_created_ts, review_answer_ts,
            review_response_hours, review_comment_title, event_ts, event_date, customer_id,
            seller_id, source_system, schema_version, ingestion_ts
        from mart.stg_fact_reviews
        on conflict (event_id)
        do update set
            review_id = excluded.review_id,
            order_id = excluded.order_id,
            review_score = excluded.review_score,
            review_created_ts = excluded.review_created_ts,
            review_answer_ts = excluded.review_answer_ts,
            review_response_hours = excluded.review_response_hours,
            review_comment_title = excluded.review_comment_title,
            event_ts = excluded.event_ts,
            event_date = excluded.event_date,
            customer_id = excluded.customer_id,
            seller_id = excluded.seller_id,
            source_system = excluded.source_system,
            schema_version = excluded.schema_version,
            ingestion_ts = excluded.ingestion_ts;
        truncate table mart.stg_fact_reviews;
    """,
    "mart.fact_order_items": """
        insert into mart.fact_order_items (
            order_id, order_item_id, product_id, seller_id, shipping_limit_date,
            price, freight_value, event_date
        )
        select
            order_id, order_item_id, product_id, seller_id, shipping_limit_date,
            price, freight_value, event_date
        from mart.stg_fact_order_items
        on conflict (order_id, order_item_id)
        do update set
            product_id = excluded.product_id,
            seller_id = excluded.seller_id,
            shipping_limit_date = excluded.shipping_limit_date,
            price = excluded.price,
            freight_value = excluded.freight_value,
            event_date = excluded.event_date;
        truncate table mart.stg_fact_order_items;
    """,
}


STAGING_DDL = {
    "mart.stg_dim_customer": "create table if not exists mart.stg_dim_customer (like mart.dim_customer including defaults);",
    "mart.stg_dim_seller": "create table if not exists mart.stg_dim_seller (like mart.dim_seller including defaults);",
    "mart.stg_dim_product": "create table if not exists mart.stg_dim_product (like mart.dim_product including defaults);",
    "mart.stg_fact_order_events": "create table if not exists mart.stg_fact_order_events (like mart.fact_order_events including defaults);",
    "mart.stg_fact_payments": "create table if not exists mart.stg_fact_payments (like mart.fact_payments including defaults);",
    "mart.stg_fact_delivery_events": "create table if not exists mart.stg_fact_delivery_events (like mart.fact_delivery_events including defaults);",
    "mart.stg_fact_reviews": "create table if not exists mart.stg_fact_reviews (like mart.fact_reviews including defaults);",
    "mart.stg_fact_order_items": "create table if not exists mart.stg_fact_order_items (like mart.fact_order_items including defaults);",
}

STAGING_MAP = {
    "mart.dim_customer": "mart.stg_dim_customer",
    "mart.dim_seller": "mart.stg_dim_seller",
    "mart.dim_product": "mart.stg_dim_product",
    "mart.fact_order_events": "mart.stg_fact_order_events",
    "mart.fact_payments": "mart.stg_fact_payments",
    "mart.fact_delivery_events": "mart.stg_fact_delivery_events",
    "mart.fact_reviews": "mart.stg_fact_reviews",
    "mart.fact_order_items": "mart.stg_fact_order_items",
}
