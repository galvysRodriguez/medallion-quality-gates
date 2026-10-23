{{ config(materialized='view') }}
{{ select_current_batch(source('streaming_raw', 'users')) }}
