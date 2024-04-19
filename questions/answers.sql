--Answer Part 2a.1

-- To aggregate data from the LINE_ITEMS and ORDERS tables, 
-- I utilized the order_id column to establish connections between line items and orders. Next, 
-- I enriched this dataset by joining the PRODUCTS table to retrieve product names based on the item_id, 
-- which serves as a unique key linking to the id column in the PRODUCTS table.

select 
    prod.id as product_id,
    prod.name as product_name,
    count(lin.order_id) as num_times_in_successful_orders
from
    alt_school.orders ord
join
    alt_school.line_items lin 
on  ord.order_id = lin.order_id
join
    alt_school.products prod 
on  lin.item_id = prod.id
where
    ord.status = 'success'      -- picking 'success' as the status for a successfully orders 
group by
    prod.id, prod.name
order by
    num_times_in_successful_orders desc
LIMIT 1;                        -- this query returns only the single most ordered item




--Answer part 2a.2
-- I performed a series of joins, starting with linking the ORDERS table to the CUSTOMERS table using the customer_id column to associate orders with customers. 
-- Subsequently, I joined the PRODUCTS table to acquire the price of each product. The data was then aggregated by customer and their location, 
-- summing up the products purchased. This process provided valuable insights into customer spending patterns based on location. 

select
    cus.customer_id,
    cus.location,
    sum(prod.price) as total_spend
from
    alt_school.orders ord
join
    alt_school.customers cus 
on  ord.customer_id = cus.customer_id
join
    alt_school.line_items lin 
on  ord.order_id = lin.order_id
join
    alt_school.products prod 
on  lin.item_id = prod.id

group by
    cus.customer_id, cus.location
order by
    total_spend desc
limit 5;                              -- this query returns only the top 5 spender.



-- Answer 2b.1
-- Initially, I joined the EVENTS table with the CUSTOMERS table using the customer_id column to correlate events with customers. 
-- I then filtered the events to include only those with a status of 'success' and an action of 'checkout,' 
-- denoting successful checkouts. I specifically accounted for checkout events with statuses other than 'success' to ensure accuracy

select
    cus.location as location,
    count(*) as checkout_count
from
    alt_school.events evt
join
    alt_school.customers cus 
on  evt.customer_id = cus.customer_id
where
    evt.event_data ->> 'status' = 'success' and  evt.event_data ->>'event_type' = 'checkout'      -- Convert JSON data to text and compare with string
group by
    cus.location
order by
    checkout_count desc
limit 1;                          -- location with the highest checkout count


-- Answer  2b.2
-- Recognizing the absence of a distinct event type for cart abandonment, I identified instances of cart abandonment by considering "checkout" events that were not successful. 
-- I created a Common Table Expression (CTE) named 'Abandoned_Carts' to isolate instances of cart abandonment before proceeding to select the customer_id and count of events for each customer. 

with abandoned_carts as (
    select
        customer_id,
        count(*) as num_events
    from
        alt_school.events 
    where
        event_data->>'event_type' = 'checkout'  
        and event_data->>'status' <> 'success'  
        and event_data->>'event_type' <> 'visit' 
        and customer_id IS NOT NULL 
    group by
        customer_id
)

select
    abandoned_carts.customer_id,
    abandoned_carts.num_events
FROM
    abandoned_carts;
    

-- Answer 2b.3
-- To compute the average number of visits per user for customers who completed a checkout, 
-- I first identified these customers using a CTE named 'Checkout_Customers.' Then, 
-- within a subquery, I selected the customer_id and counted the number of visit events for each customer who completed a checkout, filtering only visit events. Finally, 
-- I calculated the average number of visits per user by dividing the total number of visits by the number of customers who completed a checkout
   
with completed_checkouts as (
    select distinct customer_id
    from alt_school.events
    where event_data ->> 'status' = 'success'              -- Filter by successful transactions
    and event_data ->> 'event_type' = 'checkout'           -- Filter by checkout events
)

-- this calculated the number of visits for each customer who completed a checkout and rounding up to two decimal place.
select  
    avg(visit_count)::numeric(10, 2) as average_visits  
from (
    select 
        customer_id,
        count(*) as visit_count 
    from 
        alt_school.events
    where 
        customer_id in (select customer_id from completed_checkouts)             -- customers who completed a checkout
        and event_data ->> 'event_type' = 'visit'                                -- identify by visit events
    group by 
        customer_id                                                              -- the group by customer_id, calculated visit count per customer
) as visit_counts;
