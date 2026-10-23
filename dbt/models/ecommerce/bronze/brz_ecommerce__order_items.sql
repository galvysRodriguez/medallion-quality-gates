{{ config(materialized='view') }}
{{ select_current_batch(source('ecommerce_raw', 'order_items')) }}
