with

    source as (select * from {{ source("stripe", "payment") }}),

    transformed as (

        select

            id as payment_id,
            orderid as order_id,
            paymentmethod as payment_method,
            created as payment_created_at,
            status as payment_status,
            {{ cents_to_dollars("amount", 2) }} as payment_amount,
            created as created_at

        from source

    )

select *
from transformed
