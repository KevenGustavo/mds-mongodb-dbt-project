{{ config(
    materialized='table',
    schema='gold' 
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stage_ecommerce_orders') }}
),

daily_performance AS (
    SELECT
        DATE_TRUNC('day', processed_at) AS sale_date,
        shipping_city,
        shipping_state,
        
        -- Métricas de Negócio
        COUNT(DISTINCT transaction_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(quantity) AS total_items_sold,
        
        -- Cálculo de Faturamento Real: (Preço * Quantidade) aplicando a porcentagem de desconto
        SUM(unit_price * quantity) AS gross_revenue,
        SUM((unit_price * quantity) * (1 - discount_applied)) AS net_revenue
        
    FROM staging
    GROUP BY 1, 2, 3
)

SELECT 
    sale_date,
    shipping_city,
    shipping_state,
    total_orders,
    unique_customers,
    total_items_sold,
    ROUND(gross_revenue, 2) AS gross_revenue,
    ROUND(net_revenue, 2) AS net_revenue,
    
    -- Ticket Médio por transação naquele dia/cidade
    ROUND((net_revenue / total_orders), 2) AS average_ticket

FROM daily_performance
ORDER BY sale_date DESC, net_revenue DESC