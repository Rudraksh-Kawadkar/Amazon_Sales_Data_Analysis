# Amazon_Sales_Data_Analysis

Big Data Analysis Project
________________________________________
Data
Data consists of Ecommerce data from 04-09-2016 to 03-09-2018, which is about 2 years of data. The dataset we have used is a combination of 9 sub-datasets which originally is 120.3 MB sized dataset. But we have pre-processed and removed many unwanted feature columns and used the modified dataset for our project analysis.
Dataset rows : 1,16,573 
Dataset columns : 21 
Dataset size : 27.4 MB 
________________________________________
Data Description
S.No	Name	Description
1	order_id	unique id for each order (32 fixed-size number)
2	customer_id	unique id for each customer (32 fixed-size number)
3	quantity	1-21
4	price_MRP	cost price, 0.85-6735
5	payment	selling price, 0-13664.8
6	timestamp	order purchase time (local, day-month-year hour:min:sec AM/PM)
7	rating	1-5
8	product_category	category under which product belongs
9	product_id	unique id for each product (32 fixed-size number)
10	payment_type	Type of payment - credit card/debit card/boleto/voucher
11	order_status	delivered/shipped/invoiced
12	product_weight_g	weight of product (in grams), 0-40425
13	product_length_cm	length of product (in centimeter), 7-105
14	product_height_cm	height of product (in centimeter), 2-105
15	product_width_cm	width of product (in centimeter), 6-118
16	customer_city	city where order is placed
17	customer_state	state where order is placed
18	seller_id	unique id for each seller (32 fixed-size number)
19	seller_city	city where order is picked up
20	seller_state	state where order is picked up
21	payment_installments	no. of installments taken by customer to pay bill, 0-24
________________________________________
Analysis Performed using Spark
1.	Customer Segmentation
Categorizing customers based on their spendings

2.	Monthly Trend Forecasting
Visualising the monthly trend of sales

3.	Hourly Sales Analysis
Which hour has more no. of sales?

4.	Product Based Analysis
Which category product has sold more?
Which category product has more rating?
Which product has sold more?
Top 10 highest & least product rating?
Order Count for each rating

5.	Payment Preference
What are the most commonly used payment types?
Count of Orders With each No. of Payment Installments

6.	Potential Customer's Location
Where do most customers come from?

7.	Seller Rating
Which seller sold more?
Which seller got more rating?

8.	Logistics based Optimization Insights
Which city buys heavy weight products and low weight products?
How much products sold within seller state?
Machine Learning Model:

9.	Predicting future sales
ML - Linear regression
