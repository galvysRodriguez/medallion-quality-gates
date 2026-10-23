{{ config(materialized='view') }}
{{ select_current_batch(source('fintech_raw', 'accounts')) }}
