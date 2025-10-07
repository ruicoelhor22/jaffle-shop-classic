with costumers as  (

    select * from {{ ref('stg_jaffle_shop__costumers') }}

),

orders as  ( 

    -- select * from {{ ref('stg_jaffle_shop__orders') }}
    select * from {{ ref('fct_orders') }}

),

costumer_orders as (

    select 
        costumer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value

    from orders
    group by 1

),

final as (

    select
        costumers.costumer_id,
        costumers.first_name,
        costumers.last_name,
        costumer_orders.first_order_date,
        costumer_orders.most_recent_order_date,
        coalesce(costumer_orders.number_of_orders, 0) as number_of_orders,
        costumer_orders.lifetime_value

    from costumers 
    left join costumer_orders using (costumer_id)

)

select * from final