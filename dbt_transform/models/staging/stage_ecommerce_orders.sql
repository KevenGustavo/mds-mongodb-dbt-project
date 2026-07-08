{{ config(
    materialized='table',
    unique_key='transaction_id'
) }}

WITH raw_data AS (
    SELECT 
        document,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY document->>'transaction_id' 
            ORDER BY ingested_at DESC
        ) as rn
    FROM {{ source('bronze', 'ecommerce_orders_raw') }}
),

deduplicated AS (
    SELECT document 
    FROM raw_data 
    WHERE rn = 1 
),

flattened_json AS (
    SELECT
        document->>'transaction_id' AS transaction_id,
        (document->'processed_at'->>'$date')::TIMESTAMP   AS processed_at,
        
        -- Dados do Cliente
        document->'customer'->>'customer_id' AS customer_id,
        (document->'customer'->'profile'->>'age')::INTEGER AS customer_age,
        (document->'customer'->'profile'->>'is_premium')::BOOLEAN AS is_premium,
        
        document->'customer'->'shipping_address'->>'city' AS raw_city,
        document->'customer'->'shipping_address'->>'state' AS shipping_state,
        
        -- Dados de Pagamento
        document->'payment'->>'method' AS payment_method,
        document->'payment'->>'status' AS payment_status,
        
        -- Expandindo o Array de Produtos (Unnest)
        jsonb_array_elements(document->'cart') AS cart_item
    FROM deduplicated  
)

SELECT
    transaction_id,
    processed_at,
    customer_id,
    customer_age,
    is_premium,
    CASE
        WHEN UPPER(TRIM(raw_city)) IN ('SÃO LUÍS', 'SAO LUIS') THEN 'São Luís'
        WHEN UPPER(TRIM(raw_city)) IN ('SÃO PAULO', 'SP') THEN 'São Paulo'
        ELSE raw_city
    END AS shipping_city,
    shipping_state,
    payment_method,
    payment_status,
    
    -- Extraindo os dados do item cart achatado
    cart_item->>'sku' AS sku,
    cart_item->>'product_name' AS product_name,
    
    ABS((cart_item->'financials'->>'unit_price')::NUMERIC) AS unit_price,
    ABS((cart_item->'financials'->>'quantity')::INTEGER) AS quantity,
    (cart_item->'financials'->>'discount_applied')::NUMERIC AS discount_applied

FROM flattened_json
WHERE payment_status IN ('COMPLETED', 'REFUNDED')