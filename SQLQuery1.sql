select * from dbo.categories;
select * from dbo.customers;
select * from dbo.payments;
select * from dbo.inventory;
select * from dbo.order_items;
select * from dbo.orders;
select * from dbo.product_reviews;
select * from dbo.products;
select * from dbo.shippings;
select * from dbo.suppliers;


-------------------Customer an-------------------

----List the top 10 customers by total spend.

select
top 10
c.customer_id, sum(try_cast(p.amount as decimal (18,2))) total_amount , count(*) as total_payments
from dbo.customers c
join dbo.payments p
on c.customer_id = p.customer_id
group by c.customer_id 
order by total_amount desc

---------top 10 customer by rank for more accurate result...........


with ranked_customers AS(

select c.customer_id , sum(try_cast(p.amount as decimal (18,2))) total_amount , count(*) as payment_amount, 
RANK()over(order by sum(try_cast(p.amount as decimal (18,2))) desc)AS rnk_no
from dbo.customers c
join dbo.payments p
on c.customer_id = p.customer_id
group by c.customer_id
)
select *
from 
ranked_customers
where rnk_no < = 10
ORDER BY rnk_no, total_amount DESC;


------Identify customers who haven't placed any orders in the last 6 months.
select 
c.customer_id, c.city
from dbo.customers c
left join 
dbo.orders o
on c.customer_id = o.customer_id
and try_cast(order_date as date) > = dateadd(month, -6, getdate())
where o.customer_id IS NULL;

-------Calculate the average order value per customer.

select 
o.order_id, 
sum(try_cast(oi.quantity as decimal(18,2))) *sum(try_cast(oi.unit_price as decimal(18,2)))/count(try_cast(o.order_id as decimal(18,2))) as average_order_value
from dbo.orders o
join dbo.order_items oi
on o.order_id = oi.order_id
group by o.order_id
order by average_order_value desc


-------Find customers who only gave 5-star product reviews.
select product_id, customer_id
from  dbo.product_reviews
where 
rating = 5



------Identify customers who ordered more than 5 times but never reviewed a product.
select * from dbo.orders;
select * from dbo.payments;
select * from dbo.product_reviews;
select 
c.customer_id, c.first_name , c.last_name ,o.order_id, count(distinct O.order_id) as total_orders 
from dbo.customers c	
join 
dbo.orders o
on o.customer_id = c.customer_id
left join dbo.product_reviews pr
on c.customer_id = pr.customer_id
group by c.customer_id, c.first_name , c.last_name ,o.order_id
having count(distinct o.order_id) >5 and count(pr.rating) =0


-----Rank customers based on number of products bought (dense_rank).

select c.customer_id, c.first_name, c.last_name ,
dense_rank()over(order by sum(try_cast(oi.quantity as decimal (18,2))) desc) as rnk
from dbo.customers c
join dbo.orders o
on c.customer_id =o.customer_id
join dbo.order_items oi
on oi.order_id = o.order_id
GROUP BY 
    c.customer_id, 
    c.first_name, 
    c.last_name
ORDER BY 
    rnk;

-------Perform RFM (Recency, Frequency, Monetary) analysis for each customer.

select c.customer_id, datediff(day, max(try_cast(o.order_date as date), getdate()) as recency_days,
count(try_cast(o.order_id as decimal (18,2))) as frquency,
sum(try_cast(o.order_id as decimal (18,2))) as monetary
from dbo.customers c 
join dbo.orders o
on c.customer_id = o.customer_id
group by 
c.customer_id;


-------Find the top 5 best-selling products by quantity sold.


 with ranked_products as(
select order_id, product_id,
sum(try_cast(quantity as decimal (18,2))) as total_quantity_sold,
RANK()over(order by sum(try_cast(quantity as decimal (18,2)))desc) as rnk
from dbo.order_items
group by product_id,order_id
)
select order_id, product_id
from ranked_products
where rnk <=5

--------------------by group by -----------another way-----

select top 5
product_id , order_id , sum(try_cast(quantity as decimal(18,2))) as total_quantity_sold
from dbo.order_items
group by 
product_id , order_id 
order by 
total_quantity_sold desc


----------Identify products with inventory less than 10 units and high sales in the last month.

select 
i.product_id, i.stock_quantity, 
sum(try_cast(oi.quantity as decimal (18,2))) as total_quantity_sold
from dbo.inventory i
join dbo.order_items oi
on i.product_id = oi.product_id 
join
dbo.orders o
on oi.order_id = o.order_id
where i.stock_quantity<10
 
 AND o.order_date >= DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0)  -- first day of last month
  AND o.order_date < DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)       -- first day of this month

group by i.product_id , i.stock_quantity
having sum(try_cast(oi.quantity as decimal(18,2))) >50-----high sales condition 


------Get average rating per product and filter for those with >4 stars.

select product_id , avg(try_cast(rating as decimal (18,2))) as avg_rating 
from dbo.product_reviews
group by product_id 
having avg(try_cast(rating as decimal (18,2))) >4


-------Identify products that have never been ordered.


select i.product_id 
FROM dbo.inventory i
where i.product_id NOT IN (
select distinct product_id
from dbo.order_items
where
product_id  IS NOT NULL
);


-----------List all products that have been out of stock recently (e.g., stock_quantity = 0).
SELECT product_id, stock_quantity
FROM dbo.inventory
WHERE stock_quantity = 0;

----------Join with categories to find the best-selling category.


select p.category_id ,  
sum(try_cast(oi.quantity as decimal (18,2))*try_cast(oi.unit_price as decimal(18,2)))as best_selling_product
from dbo.products p
join dbo.order_items oi
on oi.product_id = p.product_id 
group by p.category_id
order by best_selling_product desc



---------Get products that are priced above the average in their category.

select 
    p.product_id, 
    p.product_name, 
    p.category_id, 
    oi.unit_price
from dbo.products p
join dbo.order_items oi
    on p.product_id = oi.product_id
where oi.unit_price > (
    select avg(try_cast(oi2.unit_price as decimal(18,2)))
    from dbo.order_items oi2
    join dbo.products p2
        on oi2.product_id = p2.product_id
    where p2.category_id = p.category_id
);




------Calculate monthly total sales.

select	
year(try_cast (o.order_date as date )) as sales_year,
month(try_cast (o.order_date as date)) as sales_month, 
sum(try_cast( oi.quantity as decimal(18,2))*try_cast(oi.unit_price as decimal(18,2))) as total_sales
from dbo.orders o
join dbo.order_items oi
on o.order_id = oi.order_id
group by 
year(try_cast (o.order_date as date )), 
month(try_cast (o.order_date as date)) 
order by 
sales_year, sales_month



-------Find the average number of products per order.

select 
avg(product_count) as avg_product_per_order
from
(
select o.order_id , 
count(try_cast(oi.product_id as decimal (18,2))) as product_count 
from dbo.orders o
join order_items oi 
on 
o.order_id = oi.order_id 
group by o.order_id 
)
as 
sub;

----Detect the percentage of orders that were cancelled.


select
(count( case when status = 'Cancelled' THEN 1 END )*100/count(*))
AS cancelled_percentage 
from dbo.orders

-----Compare order volume between different shipping methods.


select 
shipping_id , shipping_method, 
count(*) AS total_orders 
from dbo.shippings
group by 
shipping_id , shipping_method
order by total_orders desc;


-----Find orders with unusually high total value (e.g., > ₹25,000).

SELECT 
    o.order_id, 
    o.customer_id, 
    o.order_date,
    SUM(TRY_CAST(oi.quantity AS DECIMAL(18,2)) * TRY_CAST(oi.unit_price AS DECIMAL(18,2))) AS total_order_value
FROM dbo.orders o
JOIN dbo.order_items oi 
    ON o.order_id = oi.order_id
GROUP BY o.order_id, o.customer_id, o.order_date
HAVING SUM(TRY_CAST(oi.quantity AS DECIMAL(18,2)) * TRY_CAST(oi.unit_price AS DECIMAL(18,2))) > 25000
ORDER BY total_order_value DESC;



-----Detect duplicate orders placed by same customer within 1 hour.

----
SELECT 
    customer_id,
    TRY_CONVERT(DATE, order_date, 105) AS order_date,
    COUNT(*) AS duplicate_orders
FROM dbo.orders
GROUP BY customer_id, TRY_CONVERT(DATE, order_date, 105)
HAVING COUNT(*) > 1
ORDER BY duplicate_orders DESC;



------Calculate average delivery time per shipping method.

select 
s.shipping_method , 
avg(datediff(day, try_convert(date, o.order_date , 105), try_convert(date , s.delivery_date, 105))) as avg_delivery_days
FROM dbo.orders o
JOIN dbo.shippings s
on o.shipping_id = s.shipping_id
group by 
s.shipping_method
order by avg_delivery_days;

-------Find the shipping method with the fastest average delivery time.

select top 1
s.shipping_method, 
avg(datediff(day, try_convert(date, o.order_date, 105), try_convert(date, s.delivery_date, 105))) as avg_delivery_days
from dbo.orders o
join dbo.shippings s
on o.shipping_id = s.shipping_id
group by s.shipping_method
order by avg_delivery_days asc


-----Identify delayed deliveries (where delivery_date > shipping_date + 7).


select o.order_id , o.customer_id ,
try_convert(date, o.order_date , 105) AS order_date ,
try_convert(date , s.delivery_date , 105) as delivery_date,
datediff(day, try_convert(date, o.order_date , 105) , try_convert(date , s.delivery_date , 105)) as delivery_days
from dbo.orders o
join 
dbo.shippings s 
on o.shipping_id = s.shipping_id
 where datediff(day, try_convert(date, o.order_date , 105) , try_convert(date , s.delivery_date , 105)) > 7
 order by delivery_days;



------Count number of deliveries per city.

select c.city, count(*) as total_deliveries
from dbo.customers c
join dbo.orders o
on o.customer_id = c.customer_id
join
dbo.shippings s 
on o.shipping_id = s.shipping_id
group by c.city
order by 
total_deliveries;

-------Average shipping cost per category (via joining with products → orders).


SELECT 
    c.category_name,
    AVG(try_cast(s.shipping_cost as decimal(18,2))) AS avg_shipping_cost
FROM orders o
JOIN order_items oi 
    ON o.order_id = oi.order_id
JOIN products p
    ON oi.product_id = p.product_id
JOIN categories c
    ON p.category_id = c.category_id
JOIN shippings s
    ON o.shipping_id = s.shipping_id
GROUP BY c.category_name
ORDER BY avg_shipping_cost DESC;


-------Count failed or zero-value payments (if applicable).
SELECT 
    COUNT(*) AS failed_or_zero_payments
FROM payments
WHERE payment_status = 'Failed'
   OR amount = 0;


--Total revenue collected through UPI vs Credit Card.

select 
payment_id, payment_method,sum(try_cast(amount as decimal (18,2))) as Total_revenue_collected
from dbo.payments
WHERE payment_method IN ('UPI', 'Credit Card')
group by payment_id , payment_method
order by Total_revenue_collected desc


--Average payment amount per method.

select payment_method,  avg(try_cast(amount as decimal (18,2))) as avg_payment_amount
from dbo.payments
group by  payment_method
order by avg_payment_amount desc

--Detect customers who paid multiple times for the same order ID.

SELECT 
    o.customer_id,
    o.order_id,
    COUNT(p.payment_id) AS payment_count,
    SUM(TRY_CAST(p.amount AS DECIMAL(18,2))) AS total_amount_paid
FROM dbo.orders o
JOIN dbo.payments p 
    ON o.customer_id = p.customer_id  -- linking via customer
GROUP BY o.customer_id, o.order_id
HAVING COUNT(p.payment_id) > 1
ORDER BY payment_count DESC, total_amount_paid DESC;


--Find products with the most reviews.

SELECT 
    product_id,
    COUNT(review_id) AS review_count
FROM dbo.product_reviews
GROUP BY product_id
ORDER BY review_count DESC;



-------Count number of reviews per rating (1 to 5).

select * from dbo.product_reviews;
SELECT 
    rating,
    COUNT(*) AS total_reviews
FROM product_reviews
GROUP BY rating
ORDER BY rating;


----Detect reviews where comment is empty or null.
SELECT review_id,
       product_id,
       customer_id,
       rating,
       comment
FROM product_reviews
WHERE comment IS NULL
   OR TRIM(comment) = '';


--Calculate average rating per supplier.

select avg(try_cast( r.rating as decimal (18,2))) as avg_rating, s.supplier_id
from dbo.suppliers s
join dbo.products p
on p.supplier_id = s.supplier_id
join dbo.product_reviews r
on  p.product_id = r.product_id
group by s.supplier_id 
order by avg_rating desc;




--Get number of reviews each customer wrote per month.
SELECT 
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    DATEFROMPARTS(
        YEAR(CONVERT(DATE, pr.review_date, 103)),  -- 103 = dd/mm/yyyy
        MONTH(CONVERT(DATE, pr.review_date, 103)),
        1
    ) AS review_month,
    COUNT(pr.review_id) AS total_reviews
FROM customers c
JOIN product_reviews pr 
    ON c.customer_id = pr.customer_id
GROUP BY 
    c.customer_id, c.first_name, c.last_name,
    DATEFROMPARTS(
        YEAR(CONVERT(DATE, pr.review_date, 103)),
        MONTH(CONVERT(DATE, pr.review_date, 103)),
        1
    )
ORDER BY 
    review_month,
    total_reviews DESC;

--🔹 Advanced SQL Concepts
---Use ROW_NUMBER() to find the first order placed by each customer.

with ranked_orders as (
select   o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
		row_number()over (  PARTITION BY o.customer_id 
            ORDER BY o.order_date ASC)
			AS rnk
	from dbo.orders o
)
select 
ro.order_id ,
ro.customer_id ,
ro.order_date,
status 
from 
ranked_orders ro
JOIN customers c
    ON ro.customer_id = c.customer_id
WHERE rnk = 1
ORDER BY ro.customer_id;



--Use a CTE to get top 3 products in each category by sales.
WITH ProductSales AS (
    -- 1st CTE: calculate total sales per product
    SELECT 
        p.product_id,
        p.product_name,
        p.category_id,
        SUM(TRY_CAST(oi.quantity AS DECIMAL(18,2)) * TRY_CAST(oi.unit_price AS DECIMAL(18,2))) AS total_sales
    FROM products p
    JOIN order_items oi ON p.product_id = oi.product_id
    GROUP BY p.product_id, p.product_name, p.category_id
),

RankedProducts AS (
    -- 2nd CTE: rank products within each category
    SELECT
        ps.*,
        ROW_NUMBER() OVER (PARTITION BY ps.category_id ORDER BY ps.total_sales DESC) AS rn
    FROM ProductSales ps
)

-- Main query: get only top 3 products per category
SELECT *
FROM RankedProducts
WHERE rn <= 3
ORDER BY category_id, total_sales DESC;


--Use a window function to calculate rolling 7-day order count.
SELECT
    order_date,
    COUNT(order_id) OVER (
        ORDER BY order_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7day_order_count
FROM orders
ORDER BY order_date;


----Use a correlated subquery to get products costlier than the average of their category.

SELECT product_id,
       product_name,
       price,
       category_id
FROM products p
WHERE price > (
    SELECT AVG(try_cast(price as decimal(18,2))) as price
    FROM products
    WHERE category_id = p.category_id
);


---Use a pivot (CASE WHEN) to show count of orders by status per month.

SELECT 
    CONVERT(VARCHAR(7), CONVERT(DATE, order_date, 105), 120) AS order_month,
    COUNT(CASE WHEN status = 'Processing' THEN 1 END) AS Processing,
    COUNT(CASE WHEN status = 'Shipped' THEN 1 END) AS Shipped,
    COUNT(CASE WHEN status = 'Delivered' THEN 1 END) AS Delivered,
    COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) AS Cancelled
FROM orders
GROUP BY CONVERT(VARCHAR(7), CONVERT(DATE, order_date, 105), 120)
ORDER BY order_month;




----Which supplier generates the most revenue?
select * from dbo.suppliers;
select * from dbo.order_items
select * from dbo.products;

select  top 1
s.supplier_id , 
sum(try_cast(oi.quantity as decimal (18,2))*try_cast(oi.unit_price as decimal (18,2))) as total_revenue
from dbo.order_items oi
join dbo.products p
on p.product_id = oi.product_id
join dbo.suppliers s
on s.supplier_id = p.supplier_id
group by s.supplier_id 
order by total_revenue desc ;



----What is the return rate (cancelled orders vs total)?
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
    CAST(SUM(CASE WHEN status = 'Cancelled' THEN 1 ELSE 0 END) AS FLOAT) 
        / COUNT(*) * 100 AS return_rate_percentage
FROM orders;


----Which city generates the most revenue?

select * from dbo.suppliers;
select * from dbo.order_items
select * from dbo.products;

select  top 1
s.city , 
sum(try_cast(oi.quantity as decimal (18,2))*try_cast(oi.unit_price as decimal (18,2))) as total_revenue
from dbo.order_items oi
join dbo.products p
on p.product_id = oi.product_id
join dbo.suppliers s
on s.supplier_id = p.supplier_id
group by s.city
order by total_revenue desc ;


-----Most popular day of week to place orders?
-- Count orders by day of the week
SELECT 
    DATENAME(WEEKDAY, CONVERT(DATETIME, order_date, 105)) AS DayOfWeek,
    COUNT(*) AS TotalOrders
FROM orders
GROUP BY DATENAME(WEEKDAY, CONVERT(DATETIME, order_date, 105))
ORDER BY TotalOrders DESC;



----Average basket size (number of items per order).
SELECT
    AVG(try_cast(total_items as decimal (18,2))) AS average_basket_size
FROM (
    SELECT
        order_id,
        SUM(try_cast(quantity as decimal(18,2))) AS total_items
    FROM order_items
    GROUP BY order_id
) AS order_totals;

-----Revenue growth month-over-month.
WITH MonthlyRevenue AS (
    SELECT 
        YEAR(TRY_CONVERT(DATE, o.order_date, 105)) AS OrderYear,
        MONTH(TRY_CONVERT(DATE, o.order_date, 105)) AS OrderMonth,
        SUM(try_cast(oi.quantity as decimal(18,2)) * try_cast(oi.unit_price as decimal (18,2))) AS Revenue
    FROM dbo.orders o
    JOIN dbo.order_items oi ON o.order_id = oi.order_id
    GROUP BY YEAR(TRY_CONVERT(DATE, o.order_date, 105)),
             MONTH(TRY_CONVERT(DATE, o.order_date, 105))
)
SELECT 
    OrderYear,
    OrderMonth,
    Revenue,
    LAG(Revenue) OVER (ORDER BY OrderYear, OrderMonth) AS PreviousMonthRevenue,
    CASE
        WHEN LAG(Revenue) OVER (ORDER BY OrderYear, OrderMonth) = 0 THEN NULL
        ELSE ROUND(
            (Revenue - LAG(Revenue) OVER (ORDER BY OrderYear, OrderMonth)) * 100.0
            / LAG(Revenue) OVER (ORDER BY OrderYear, OrderMonth), 2
        )
    END AS MoM_Growth_Percent
FROM MonthlyRevenue
ORDER BY OrderYear, OrderMonth;



-----Customers with increasing order frequency over the last 3 months.

WITH MonthlyOrders AS (
    SELECT 
        customer_id,
        DATEFROMPARTS(
            YEAR(TRY_CONVERT(DATE, order_date, 105)),
            MONTH(TRY_CONVERT(DATE, order_date, 105)),
            1
        ) AS order_month,
        COUNT(order_id) AS order_count
    FROM orders
    WHERE TRY_CONVERT(DATE, order_date, 105) IS NOT NULL
    GROUP BY customer_id,
             DATEFROMPARTS(
                YEAR(TRY_CONVERT(DATE, order_date, 105)),
                MONTH(TRY_CONVERT(DATE, order_date, 105)),
                1
             )
),
Last3Months AS (
    SELECT 
        mo.customer_id,
        mo.order_month,
        mo.order_count,
        ROW_NUMBER() OVER (PARTITION BY mo.customer_id ORDER BY mo.order_month DESC) AS rn
    FROM MonthlyOrders mo
    WHERE mo.order_month >= DATEADD(MONTH, -3, GETDATE())
),
Pivoted AS (
    SELECT
        customer_id,
        MAX(CASE WHEN rn = 3 THEN order_count END) AS month1_orders,
        MAX(CASE WHEN rn = 2 THEN order_count END) AS month2_orders,
        MAX(CASE WHEN rn = 1 THEN order_count END) AS month3_orders
    FROM Last3Months
    GROUP BY customer_id
)
SELECT 
    customer_id,
    month1_orders,
    month2_orders,
    month3_orders
FROM Pivoted
WHERE month1_orders IS NOT NULL 
  AND month2_orders IS NOT NULL
  AND month3_orders IS NOT NULL
  AND month1_orders < month2_orders 
  AND month2_orders < month3_orders
ORDER BY customer_id;

----Find top 10 profitable product (total revenue minus shipping cost per order).

select top 10
oi.product_id , sum(try_cast(oi.quantity as decimal(18,2)) * try_cast(oi.unit_price as decimal (18,2)) - try_cast(s.shipping_cost as decimal (18,2))) as total_profit 
from dbo.order_items oi
join dbo.orders o 
on o.order_id  = oi.order_id 
Join dbo.shippings s
on s.shipping_id = o.shipping_id 
group by oi.product_id
order by total_profit desc;

----Customer Segmentation & Behavior




-----Segment customers based on total amount spent (High/Medium/Low).
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    SUM(try_cast(oi.quantity as decimal(18,2)) * try_cast(oi.unit_price as decimal(18,2))) AS total_spent,
    CASE 
        WHEN SUM(try_cast(oi.quantity as decimal(18,2)) * try_cast(oi.unit_price as decimal(18,2)))  >= 50000 THEN 'High'
        WHEN SUM(try_cast(oi.quantity as decimal(18,2)) * try_cast(oi.unit_price as decimal(18,2)))  >= 20000 THEN 'Medium'
        ELSE 'Low'
    END AS spending_segment
FROM customers c
JOIN orders o 
    ON c.customer_id = o.customer_id
JOIN order_items oi 
    ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC;

----Identify repeat customers who placed more than 2 orders every month.

select * from dbo.orders
SELECT 
    customer_id, 
    MONTH(TRY_CONVERT(date, order_date, 105)) AS order_month,
    YEAR(TRY_CONVERT(date, order_date, 105)) AS order_year,
    COUNT(order_id) AS total_orders
FROM dbo.orders
GROUP BY 
    customer_id, 
    MONTH(TRY_CONVERT(date, order_date, 105)), 
    YEAR(TRY_CONVERT(date, order_date, 105))
HAVING COUNT(order_id) > 2
ORDER BY 
    customer_id,
    order_year,
    order_month;


-----Find customers who ordered from at least 3 different categories.

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT p.category_id) AS category_count
FROM customers c
JOIN orders o 
    ON c.customer_id = o.customer_id
JOIN order_items oi 
    ON o.order_id = oi.order_id
JOIN products p 
    ON oi.product_id = p.product_id
JOIN categories cat 
    ON p.category_id = cat.category_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(DISTINCT p.category_id) >= 3
ORDER BY category_count DESC;


---Identify customers with more than one shipping city in order history.


SELECT 
    o.customer_id,
    COUNT(DISTINCT s.city) AS unique_shipping_cities
FROM dbo.orders o 
join dbo.order_items oi
on o.order_id = oi.order_id 
JOIN dbo.products p
ON oi.product_id = p.product_id
JOIN dbo.suppliers s
on s.supplier_id = p.supplier_id 
GROUP BY o.customer_id
HAVING COUNT(DISTINCT s.city) > 1
ORDER BY unique_shipping_cities DESC;



----Find top customers who ordered using multiple payment methods.
--
SELECT top 10
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT p.payment_method) AS distinct_payment_methods
FROM dbo.customers c
JOIN dbo.orders o 
    ON c.customer_id = o.customer_id
JOIN dbo.payments p 
    ON o.payment_id = p.payment_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(DISTINCT p.payment_method) > 1



----Track customer acquisition trends by month (from created_at).
select * from dbo.customers;

SELECT 
    FORMAT(TRY_CONVERT(DATE, created_at), 'yyyy-MM') AS Month_Year,
    COUNT(customer_id) AS New_Customers
FROM customers
WHERE TRY_CONVERT(DATE, created_at) IS NOT NULL  -- Ignore bad dates
GROUP BY FORMAT(TRY_CONVERT(DATE, created_at), 'yyyy-MM')
ORDER BY Month_Year;


----Get average number of days between orders per customer.


WITH OrderGaps AS (
    SELECT
        customer_id,
        order_id,
        TRY_CONVERT(DATE, order_date, 103) AS OrderDate,  -- 103 = dd/MM/yyyy format
        LAG(TRY_CONVERT(DATE, order_date, 103)) OVER (
            PARTITION BY customer_id 
            ORDER BY TRY_CONVERT(DATE, order_date, 103)
        ) AS Prev_Order_Date
    FROM orders
    WHERE TRY_CONVERT(DATE, order_date, 103) IS NOT NULL  -- Skip bad date rows
)
SELECT
    customer_id,
    AVG(DATEDIFF(DAY, Prev_Order_Date, OrderDate)) AS Avg_Days_Between_Orders
FROM OrderGaps
WHERE Prev_Order_Date IS NOT NULL
GROUP BY customer_id
ORDER BY Avg_Days_Between_Orders;


---Identify customers who placed their first order on a weekend.

SELECT 
    o.customer_id,
    MIN(o.order_date) AS first_order_date,
    DATENAME(WEEKDAY, TRY_CONVERT(DATE, MIN(o.order_date))) AS first_order_day
FROM orders o
GROUP BY o.customer_id
HAVING DATENAME(WEEKDAY, TRY_CONVERT(DATE, MIN(o.order_date))) IN ('Saturday', 'Sunday');


----Detect churned customers (no orders in last 90 days).
SELECT 
    c.customer_id,
    c.first_name,
    c.last_name
FROM customers c
LEFT JOIN orders o 
    ON c.customer_id = o.customer_id
WHERE 
    TRY_CONVERT(DATE, o.order_date, 103) IS NULL
    OR TRY_CONVERT(DATE, o.order_date, 103) < DATEADD(DAY, -90, CAST(GETDATE() AS DATE));

----Which customers have never used COD?
SELECT DISTINCT c.customer_id
FROM customers c
LEFT JOIN orders o 
    ON c.customer_id = o.customer_id
LEFT JOIN payments p 
    ON o.payment_id = p.payment_id
    AND p.payment_method = 'COD'
WHERE p.payment_id IS NULL;

----Find frequently bought together product pairs (using self-join on order_items).
SELECT 
    oi1.product_id AS product_1,
    oi2.product_id AS product_2,
    COUNT(*) AS times_bought_together
FROM order_items oi1
JOIN order_items oi2 
    ON oi1.order_id = oi2.order_id
    AND oi1.product_id < oi2.product_id
GROUP BY oi1.product_id, oi2.product_id
ORDER BY times_bought_together DESC;

