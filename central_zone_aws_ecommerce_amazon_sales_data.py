# Databricks notebook source
#importing modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

#creating spark session object
spark = SparkSession.builder.appName("czaeasd").getOrCreate()

# COMMAND ----------

#creating dataframe from the data source 
sales_data_df=spark.read.format("csv").option("header","true").option('inferSchema', True).load('/FileStore/tables/central_zone_aws_ecommerce_amazon_sales_data.csv')
sales_data_df.cache()
#it seems that the timestamp is not correct so changing the 
#timestamp to datetime and getting the date, month, year of the timestamp
sales_data_df = sales_data_df.withColumn('timestamp_array',f.split(f.col("timestamp")," "))
sales_data_df = sales_data_df.withColumn('order_date', f.col('timestamp_array')[0])
sales_data_df = sales_data_df.withColumn('order_date', f.split(f.col("order_date"),"-"))
sales_data_df = sales_data_df.withColumn('order_date', f.concat_ws('-', f.col('order_date')[1], f.col('order_date')[0], f.col('order_date')[2]))
sales_data_df = sales_data_df.withColumn('order_time', f.col('timestamp_array')[1])
sales_data_df = sales_data_df.withColumn('order_date', f.to_date(f.col("order_date"),"MM-dd-yyyy"))
sales_data_df = sales_data_df.drop(f.col('timestamp_array'))
sales_data_df.display()

# COMMAND ----------

# Customer Segmentation:
# Total Spend by each customer
customer_total_bill_df = sales_data_df.groupBy(f.col('customer_id').alias('Customer_ID')).agg(f.sum(f.col('quantity')*f.col('payment')).alias('Bill_Amount'))
customer_total_bill_df = customer_total_bill_df.orderBy(f.col('Bill_Amount'))
customer_total_bill_df.display()

# COMMAND ----------

# Customer Type
customer_type_df = customer_total_bill_df.withColumn("Customer_Type", f.when(f.col('Bill_Amount') >= 50000, 'VIP Customer')
                                 .when((f.col('Bill_Amount')< 50000) & (f.col('Bill_Amount') >= 20000), 'Silver Customer')
                                 .when((f.col('Bill_Amount') < 20000) & (f.col('Bill_Amount') >= 5000), 'Bronze  Customer')
                                 .otherwise('Lowtime Customer'))
customer_type_df.display()     

# COMMAND ----------

#Customer Type Category Count
customer_type_category_df = customer_type_df.groupBy(f.col('Customer_Type')).agg(f.count(f.col('Customer_Type')).alias('Total_Customers'))
customer_type_category_df = customer_type_category_df.orderBy(f.col('Total_Customers').desc())
customer_type_category_df.display()

# COMMAND ----------

# Monthly Trend Forecasting:
# -------------------------
monthly_trend_forecasting_df = sales_data_df.select(['customer_id','quantity', 'order_date', 'price_MRP'])
monthly_trend_forecasting_df = monthly_trend_forecasting_df.withColumn('order_date',f.month(f.col('order_date')))
monthly_trend_forecasting_df = monthly_trend_forecasting_df.withColumnRenamed('order_date', 'order_month')
monthly_trend_forecasting_df = monthly_trend_forecasting_df.groupBy(f.col('order_month')).agg(f.sum(f.col('quantity')).alias('Quantity'),f.count(f.col('customer_id')).alias('Customer Count'),f.sum(f.col('price_MRP')).alias('Price'))
monthly_trend_forecasting_df = monthly_trend_forecasting_df.withColumn('Price', f.round(f.col('Price'),2))
monthly_trend_forecasting_df = monthly_trend_forecasting_df.orderBy(f.col('order_month').asc())
monthly_trend_forecasting_df.display()

# COMMAND ----------

#Monthly Profit Analysis:
#------------------------
monthly_profit_df = sales_data_df.select(['order_date','payment', 'price_MRP', 'payment'])
monthly_profit_df = monthly_profit_df.withColumn('order_date',f.month(f.col('order_date')))
monthly_profit_df = monthly_profit_df.withColumnRenamed('order_date', 'Month')
monthly_profit_df = monthly_profit_df.withColumn('Net Profit', f.col('payment')-f.col('price_MRP'))
monthly_profit_df = monthly_profit_df.groupBy("Month").agg(f.sum("Net Profit").alias("Total Profit"),f.avg("Net Profit").alias("Avg Profit"),(f.sum(f.col('Net Profit')) / f.sum(f.col('payment')) * 100).alias("Profit Percentage")).orderBy("Month")
monthly_profit_df.display()


# COMMAND ----------

#Hourly sales analysis:
#----------------------
hourly_sales_df = sales_data_df.select(['order_time','quantity','payment'])
hourly_sales_df = hourly_sales_df.withColumn('Hour', f.split(f.col('order_time'),':')[0]).drop('order_time')
hourly_sales_df = hourly_sales_df.groupBy("Hour").agg(f.sum(f.col('quantity')).alias("Quantity"),(f.sum(f.col('payment')) / f.count(f.col('payment'))).alias("Avg Price")).orderBy("Hour")
hourly_sales_df.display()

# COMMAND ----------

#Product Wise Analysis:
#----------------------
product_based_analysis_df = sales_data_df.select(['product_id', 'rating','payment'])
# Calculate total number of unique products sold
total_unique_products = product_based_analysis_df.select('product_ID').distinct().count()

# Display the result
print("Total number of unique products sold:", total_unique_products)

product_based_analysis_df = product_based_analysis_df.groupBy('product_id').agg(f.avg(f.col('rating')).alias('AVG Rating'), f.avg(f.col('payment')).alias('AVG Sales'), f.count(f.col('product_id')).alias('No of Orders'))
product_based_analysis_df = product_based_analysis_df.withColumn('AVG Rating', f.round(f.col('AVG Rating'),2))
product_based_analysis_df = product_based_analysis_df.withColumn('AVG Sales', f.round(f.col('AVG Sales'),2))
product_based_analysis_df.display()

# COMMAND ----------

#Top 10 Orders
# Window specification for ranking
window_spec = Window.orderBy(f.col("No of Orders").desc())

# Add a rank column to the DataFrame
product_based_analysis_df = product_based_analysis_df.withColumn("Rank", f.rank().over(window_spec))

# Select the top 10 most sold products
top_10_most_sold = product_based_analysis_df.filter(f.col("Rank") <= 10)
top_10_most_sold = top_10_most_sold.select(['product_id', 'AVG Rating', 'AVG Sales', 'No of Orders'])
top_10_most_sold.display()


# COMMAND ----------

#Top 10 Least Orders
# Window specification for ranking
window_spec = Window.orderBy(f.col("No of Orders").asc())

# Add a rank column to the DataFrame
product_based_analysis_df = product_based_analysis_df.withColumn("Rank", f.rank().over(window_spec)).limit(10)

# Select the top 10 least sold products
top_10_least_sold = product_based_analysis_df.filter(f.col("Rank") <= 10)
top_10_least_sold = top_10_least_sold.select(['product_id', 'AVG Rating', 'AVG Sales', 'No of Orders'])
top_10_least_sold.show()

# COMMAND ----------

# Product Category Analysis:
# --------------------------

product_category_analysis_df = sales_data_df.select(['product_category', 'rating','payment'])
# Calculate total number of unique products sold
total_unique_products = product_category_analysis_df.select('product_category').distinct().count()

# Display the result
print("Total number of unique products sold:", total_unique_products)

product_category_analysis_df = product_category_analysis_df.groupBy('product_category').agg(f.avg(f.col('rating')).alias('AVG Rating'), f.avg(f.col('payment')).alias('AVG Sales'), f.count(f.col('product_category')).alias('No of Orders'))
product_category_analysis_df = product_category_analysis_df.withColumn('AVG Rating', f.round(f.col('AVG Rating'),2))
product_category_analysis_df = product_category_analysis_df.withColumn('AVG Sales', f.round(f.col('AVG Sales'),2))
product_category_analysis_df.display()


# COMMAND ----------


# Sort the DataFrame by "No of Orders" in ascending order
top_10_product_category_df = product_category_analysis_df.sort(f.col("No of Orders").asc())

# Use window functions to rank the categories based on "No. of Orders"
window_spec = Window.orderBy(f.col("No of Orders").desc())
top_ranked_category_df = top_10_product_category_df.withColumn("rank", f.rank().over(window_spec))

# Filter the top 10 categories
top_10_categories = top_ranked_category_df.filter(f.col("rank") <= 10)

# Display the result
top_10_categories.select(f.col("product_category"),f.col("Avg Rating"),f.col("Avg Sales"),f.col("No of Orders")).show()

# COMMAND ----------

# Sort the DataFrame by "No. of Orders" in descending order
top_10_least_product_category_sorted_df = product_category_analysis_df.sort(f.col("No of Orders").desc())

# Use window functions to rank the categories based on "No. of Orders"
window_spec = Window.orderBy(f.col("No of Orders").asc())
top_10_least_product_category_ranked_df = top_10_least_product_category_sorted_df.withColumn("rank", f.rank().over(window_spec))

# Filter the least 10 categories sold
least_top_10_categories = top_10_least_product_category_ranked_df.filter(f.col("rank") <= 10)

# Display the result
least_top_10_categories.select(f.col("product_category"),f.col("Avg Rating"),f.col("Avg Sales"),f.col("No of Orders")).show()

# COMMAND ----------

#Ratings given
# Select the "rating" column and calculate the count of each rating
rating_df = sales_data_df.select("rating").groupBy("rating").count()

# Display the ratings and their counts
rating_df = rating_df.withColumnRenamed("rating", "Rating").withColumnRenamed("count", "Count of Ratings")
rating_df = rating_df.orderBy(f.col('Rating'))
rating_df.display()

# COMMAND ----------

#Payment prefernce:
# ------------------

# Create RDDs payment_rdd_1 and payment_rdd_2 using DataFrame transformations

payment_rdd_1= sales_data_df.select("payment_type").groupBy("payment_type").count()
payment_rdd_1 = payment_rdd_1.withColumnRenamed("count", "Count").orderBy(f.col("Count").desc())

payment_rdd_2=  sales_data_df.select("payment_type", (f.col("payment") * f.col("quantity")).alias("total_price"))
payment_rdd_2 = payment_rdd_2.groupBy("payment_type").sum("total_price")
payment_rdd_2 = payment_rdd_2.withColumnRenamed("sum(total_price)", "Total Amount spent").orderBy(f.col("Total Amount spent").desc())

# Join RDDs and perform the required transformations
final_payment_rdd = payment_rdd_1.join(payment_rdd_2, "payment_type")
final_payment_rdd= final_payment_rdd.withColumnRenamed("Count", "Count")
final_payment_rdd= final_payment_rdd.withColumnRenamed("Total Amount spent", "Total Amount spent")

# Calculate the average amount spent per order
final_payment_rdd = final_payment_rdd.withColumn("Avg Amount spent per order", f.col("Total Amount spent") / f.col("Count"))

# Show the result
final_payment_rdd.select(f.col("payment_type").alias("Payment Type"),f.col("Count"),f.round(f.col("Total Amount spent"),2).alias("Total Amount spent"),f.round(f.col("Avg Amount spent per order"),2).alias("Avg Amount spent per order")).display()

# COMMAND ----------

# Count of Orders With each No. of Payment Installments:

# Create RDD ePairRdd12 using DataFrame transformations
order_payment_installments_df = sales_data_df.select("payment_installments").groupBy("payment_installments").count()
order_payment_installments_df = order_payment_installments_df.withColumnRenamed("payment_installments", "No of Payment Installments")
order_payment_installments_df = order_payment_installments_df.withColumnRenamed("count", "Count for each installment")
order_payment_installments_df = order_payment_installments_df.orderBy(f.col("No of Payment Installments"))

# Show the result
order_payment_installments_df.display()

# COMMAND ----------

# Create RDDs seller_ranking_rdd_1, seller_ranking_rdd_2, and seller_ranking_rdd_3 using DataFrame transformations
seller_ranking_rdd_1 = sales_data_df.select("seller_id").groupBy("seller_id").count()
seller_ranking_rdd_1= seller_ranking_rdd_1.withColumnRenamed("seller_id", "Seller ID")
seller_ranking_rdd_1= seller_ranking_rdd_1.withColumnRenamed("count", "Customers Reached")
seller_ranking_rdd_1= seller_ranking_rdd_1.orderBy(f.col("Customers Reached").desc())

seller_ranking_rdd_2 = sales_data_df.select("seller_id", (f.col("payment") * f.col("quantity")).alias("total_sales"))
seller_ranking_rdd_2= seller_ranking_rdd_2.groupBy("seller_id").sum("total_sales")
seller_ranking_rdd_2= seller_ranking_rdd_2.withColumnRenamed("seller_id", "Seller ID")
seller_ranking_rdd_2= seller_ranking_rdd_2.withColumnRenamed("sum(total_sales)", "Total Sales")
seller_ranking_rdd_2= seller_ranking_rdd_2.orderBy(f.col("Total Sales").desc())

seller_ranking_rdd_3 = sales_data_df.select("seller_id", "rating")
seller_ranking_rdd_3 = seller_ranking_rdd_3.groupBy("seller_id").sum("rating")
seller_ranking_rdd_3 = seller_ranking_rdd_3.withColumnRenamed("seller_id", "Seller ID")
seller_ranking_rdd_3 = seller_ranking_rdd_3.withColumnRenamed("sum(rating)", "Total Rating")
seller_ranking_rdd_3 = seller_ranking_rdd_3.orderBy(f.col("Total Rating").desc())

# Join the RDDs
final_seller_raking_df = seller_ranking_rdd_1.join(seller_ranking_rdd_2, "Seller ID").join(seller_ranking_rdd_3, "Seller ID")

# Calculate the average rating and round it off to two decimal places
final_seller_raking_df = final_seller_raking_df.withColumn("Avg Rating", f.round(f.col("Total Rating") / f.col("Customers Reached"), 2))

# Show the result
final_seller_raking_df.select(f.col("Seller ID"),f.col("Customers Reached"),f.round(f.col("Total Sales"),2).alias("Total Sales"),f.col("Avg Rating")).display()

# COMMAND ----------

# Potential Customer Location: - Statewise
# ----------------------------
# Create RDDs statewise_customer_rdd_1 and statewise_customer_rdd_1 using DataFrame transformations
statewise_customer_rdd_1 = sales_data_df.select("customer_state").groupBy("customer_state").count()
statewise_customer_rdd_1 = statewise_customer_rdd_1.withColumnRenamed("customer_state", "Customer State")
statewise_customer_rdd_1 = statewise_customer_rdd_1.withColumnRenamed("count", "Count")
statewise_customer_rdd_1 = statewise_customer_rdd_1.orderBy(f.col("Count").desc())

statewise_customer_rdd_2 = sales_data_df.select("customer_state", (f.col("payment") * f.col("quantity")).alias("total_amount"))
statewise_customer_rdd_2 = statewise_customer_rdd_2.groupBy("customer_state").sum("total_amount")
statewise_customer_rdd_2 = statewise_customer_rdd_2.withColumnRenamed("customer_state", "Customer State")
statewise_customer_rdd_2 = statewise_customer_rdd_2.withColumnRenamed("sum(total_amount)", "Total Amount spent")
statewise_customer_rdd_2 = statewise_customer_rdd_2.orderBy(f.col("Total Amount spent").desc())

# Join RDDs and calculate the average amount spent per order
final_statewise_customer_rdd = statewise_customer_rdd_1.join(statewise_customer_rdd_2, "Customer State")
final_statewise_customer_df = final_statewise_customer_rdd.withColumn("Avg Amount spent per order", f.col("Total Amount spent") / f.col("Count"))

# Show the result
final_statewise_customer_df.select(f.col("Customer State"),f.col("Count"),f.round(f.col("Total Amount spent"),2).alias("Total Amount spent"),f.round(f.col("Avg Amount spent per order"),2).alias("Avg Amount spent per order")).display()

# COMMAND ----------

# Potential Customer Location: - Citywise
# ----------------------------
# Create RDDs citywise_customer_rdd_1 and citywise_customer_rdd_2 using DataFrame transformations
citywise_customer_rdd_1 = sales_data_df.select("customer_city").groupBy("customer_city").count()
citywise_customer_rdd_1 = citywise_customer_rdd_1.withColumnRenamed("customer_city", "Customer City")
citywise_customer_rdd_1 = citywise_customer_rdd_1.withColumnRenamed("count", "Count")
citywise_customer_rdd_1 = citywise_customer_rdd_1.orderBy(f.col("Count").desc())

citywise_customer_rdd_2 = sales_data_df.select("customer_city", (f.col("payment") * f.col("quantity")).alias("total_amount"))
citywise_customer_rdd_2 = citywise_customer_rdd_2.groupBy("customer_city").sum("total_amount")
citywise_customer_rdd_2 = citywise_customer_rdd_2.withColumnRenamed("customer_city", "Customer City")
citywise_customer_rdd_2 = citywise_customer_rdd_2.withColumnRenamed("sum(total_amount)", "Total Amount spent")
citywise_customer_rdd_2 = citywise_customer_rdd_2.orderBy(f.col("Total Amount spent").desc())

# Join RDDs and calculate the average amount spent per order
citywise_customer_rdd = citywise_customer_rdd_1.join(citywise_customer_rdd_2, "Customer City")
citywise_customer_df = citywise_customer_rdd.withColumn("Avg Amount spent per order", f.col("Total Amount spent") / f.col("Count"))

# Show the result
citywise_customer_df.select(f.col("Customer City"),f.col("Count"),f.round(f.col("Total Amount spent"),2).alias("Total Amount spent"),f.round(f.col("Avg Amount spent per order"),2).alias("Avg Amount spent per order")).display()

# COMMAND ----------

# Logistics Based Optimization Insights:
# --------------------------------------

# Which city buys more heavy wieght products?

# Create RDD city_weight using DataFrame transformations
city_weight_1 = sales_data_df.select("customer_city", "product_width_cm").rdd.map(lambda row: (row[0], row[1])).map(lambda x: (x[0], 1) if 200 <= x[1] < 1000 else (x[0], 2) if 1000 <= x[1] < 3000 else (x[0], 3) if x[1] < 200 else (x[0], 4)).reduceByKey(lambda a, b: a + b).mapValues(lambda x: [x])

# Calculate the average weight and create city_weight_2 RDD
city_weight_2 = city_weight_1.map(lambda x: (x[0], (sum(x[1]), len(x[1])))).mapValues(lambda x: x[0] / x[1])

# Show the result with the desired column names
city_weight_2.toDF(["City", "Weight Category"]).show()

# COMMAND ----------

# Create RDD city_weight_label to assign labels based on weight category
city_weight_label = city_weight_2.map(lambda x: (x[0], "Heavy") if x[1] == 1 else (x[0], "Slightly Heavy") if x[1] == 2 else (x[0], "Medium") if x[1] == 3 else (x[0], "Light"))

# Create RDD city_weight_list to group cities by weight category and count them
city_weight_list = city_weight_label.groupByKey().map(lambda x: (x[0], list(x[1]), len(list(x[1]))))

# Show the result with the desired column names
city_weight_list.toDF(["Weight Category", "City", "City Count"]).show()

# COMMAND ----------

# How many proucts are domestic sales(Sold within seller city itself)?
sales_loc = sales_data_df.select("customer_city", "seller_city").rdd.map(lambda row: (row[0], row[1])).map(lambda x: (x[0], x[1], 1) if x[0] == x[1] else (x[0], x[1], 0)).filter(lambda x: x[0] == x[1]).map(lambda x: (x[0], x[2])).reduceByKey(lambda a, b: a + b)

# Show the result with the desired column names
sales_loc.toDF(["City", "Count"]).show()

# COMMAND ----------

# How many proucts are domestic sales(Sold within seller city itself)?
domestic_sales_count = sales_loc.map(lambda x: x[1]).sum()
foreign_sales_count = (sales_data_df.count() - domestic_sales_count)

# Create an RDD location_count to hold the counts
location_count = spark.sparkContext.parallelize([(domestic_sales_count, foreign_sales_count)])

# Convert the RDD to a DataFrame with appropriate column names and show it
location_count.toDF(["Domestic Sales", "Foreign Sales"]).show()


# COMMAND ----------

from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, Normalizer

# DataFrame transformations
inputDf = sales_data_df.withColumn("ts_split", f.split("timestamp", " ")) \
    .withColumn("d_m_y", f.split(f.col("ts_split")[0], "-")) \
    .withColumn("hour", f.split(f.col("ts_split")[1], ":")[0].cast("int")) \
    .withColumn("day", f.col("d_m_y")[0].cast("int")) \
    .withColumn("month", f.col("d_m_y")[1].cast("int")) \
    .orderBy(f.col("payment").desc())

inputDf = inputDf.select(f.col("payment").cast("float").alias("label"), "hour", "day", "month")

# Vector Assembler
assembler = VectorAssembler(inputCols=["hour", "day", "month"], outputCol="features")
assembledDf = assembler.transform(inputDf)

# Normalizer
normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
normalizedDf = normalizer.transform(assembledDf)

# Linear Regression
lr = LinearRegression(labelCol="label", featuresCol="normFeatures", maxIter=10, regParam=1.0, elasticNetParam=1.0)
lrModel = lr.fit(normalizedDf)

# Model Evaluation
predictions = lrModel.transform(normalizedDf).select("features", "label", "prediction")

# Show predictions
predictions.show()

# COMMAND ----------

# Model Coefficients and Metrics
print(f"Coefficients: {lrModel.coefficients}")
print(f"Intercept: {lrModel.intercept}")

trainingSummary = lrModel.summary
print(f"numIterations: {trainingSummary.totalIterations}")
print(f"objectiveHistory: {trainingSummary.objectiveHistory}")
print(f"RMSE: {trainingSummary.rootMeanSquaredError}")
print(f"r2: {trainingSummary.r2}")

# COMMAND ----------

trainingSummary.residuals.show()

# COMMAND ----------

# Manual Testing
testData = spark.createDataFrame([(238.61, 21, 4, 8)], ["label", "hour", "day", "month"])

# Vector Assembler for testing data
testAssembler = VectorAssembler(inputCols=["hour", "day", "month"], outputCol="features")
testAssembledData = testAssembler.transform(testData)

# Normalizer for testing data
testNormalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
testNormalizedData = testNormalizer.transform(testAssembledData)

# Make predictions for testing data
testPredictions = lrModel.transform(testNormalizedData).select("features", "label", "prediction")

# Show predictions for testing data
testPredictions.show()
