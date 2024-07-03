### APENAS ALGUNS EXEMPLOS DE CÓDIGO PARA O DASHBOARD
import time  # to simulate a real time data, time loop

import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

# read csv from a github repo
dataset_url = "https://raw.githubusercontent.com/Lexie88rus/bank-marketing-analysis/master/bank.csv"

# read csv from a URL
@st.experimental_memo
def get_data() -> pd.DataFrame:
    return pd.read_csv(dataset_url)

df = get_data()

# dashboard title
st.title("Real-Time / Live Data Science Dashboard")

# top-level filters
job_filter = st.selectbox("Select the Job", pd.unique(df["job"]))

# creating a single-element container
placeholder = st.empty()

# dataframe filter
df = df[df["job"] == job_filter]

# near real-time / live feed simulation
for seconds in range(200):

    df["age_new"] = df["age"] * np.random.choice(range(1, 5))
    df["balance_new"] = df["balance"] * np.random.choice(range(1, 5))

    # creating KPIs
    avg_age = np.mean(df["age_new"])

    count_married = int(
        df[(df["marital"] == "married")]["marital"].count()
        + np.random.choice(range(1, 30))
    )

    balance = np.mean(df["balance_new"])

    with placeholder.container():

        # create three columns
        kpi1, kpi2, kpi3 = st.columns(3)

        # fill in those three columns with respective metrics or KPIs
        kpi1.metric(
            label="Age ⏳",
            value=round(avg_age),
            delta=round(avg_age) - 10,
        )
        
        kpi2.metric(
            label="Married Count 💍",
            value=int(count_married),
            delta=-10 + count_married,
        )
        
        kpi3.metric(
            label="A/C Balance ＄",
            value=f"$ {round(balance,2)} ",
            delta=-round(balance / count_married) * 100,
        )

        # create two columns for charts
        fig_col1, fig_col2 = st.columns(2)
        with fig_col1:
            st.markdown("### First Chart")
            fig = px.density_heatmap(
                data_frame=df, y="age_new", x="marital"
            )
            st.write(fig)
            
        with fig_col2:
            st.markdown("### Second Chart")
            fig2 = px.histogram(data_frame=df, x="age_new")
            st.write(fig2)

        st.markdown("### Detailed Data View")
        st.dataframe(df)
        time.sleep(1)

#### CODIGO USADO NA A2

import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date, max as spark_max
from streamlit_option_menu import option_menu
import os
import datetime

os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk-22'

# Set dummy AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_SESSION_TOKEN'] = 'test'

# Set Hadoop home directory
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['hadoop.home.dir'] = 'C:/hadoop'

# DynamoDB table name
dynamodb_orders = "orders"
dynamodb_events = "CadeAnalyticsEvents"
dynamodb_products = "products"

# Initialize Spark session with the DynamoDB connector
spark = SparkSession.builder \
    .appName("PySparkDynamoDBExample") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375,com.audienceproject:spark-dynamodb_2.12:1.1.2") \
    .config("spark.driver.extraJavaOptions", "-Daws.dynamodb.endpoint=http://localhost:8000") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "localhost") \
    .getOrCreate()

# Load data from DynamoDB table into a DataFrame
df_orders = spark.read \
    .format("dynamodb") \
    .option("tableName", dynamodb_orders) \
    .load()

df_events = spark.read \
    .format("dynamodb") \
    .option("tableName", dynamodb_events) \
    .load()

df_products = spark.read \
    .format("dynamodb") \
    .option("tableName", dynamodb_products) \
    .load()

# Calculate 1 minute ago
one_minute_ago = datetime.datetime.now() - datetime.timedelta(minutes=1)

# calculate 1 hour ago
one_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)

# calculate total price of products sold in df_orders 
df_orders = df_orders.withColumn("total_price", col("price") * col("quantity"))

# get the unique combinations of user_id and product_id in df_events
unique_events = df_events.groupBy("store_id", "user_id", "product_id").count()

# group by store_id and sum the quantity of products sold in df_orders and the total price of products sold
stores = df_orders.groupBy("store_id").sum("quantity", "total_price")
total_products_sold = stores.withColumnRenamed("sum(quantity)", "total_products_sold")
total_products_sold = total_products_sold.withColumnRenamed("sum(total_price)", "total_price")

st.set_page_config(
    page_title="Real-Time Data Science Dashboard",
    page_icon="✅",
    layout="wide",
)

# create streamlit interface
st.title("Real-Time Data Science Dashboard")
st.write("Welcome to the Real-Time Data Science Dashboard! Here you can explore the latest data from your data sources.")

st.subheader("Total Products Sold by Store")
# Create a list of store IDs for the option menu
store_ids = sorted([row["store_id"] for row in total_products_sold.select("store_id").distinct().collect()])
store_id = st.selectbox("Select a store ID", store_ids, index=None)

if store_id:
    st.write(f"Overview of store {store_id}")

    # create columns
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)

    # Filter the DataFrame based on the selected store_id and show the result
    filtered_df = total_products_sold.filter(col("store_id") == store_id)

    # create a metric card to display the total price of products sold
    total_price = filtered_df.select("total_price").collect()[0][0]

    # format the total price
    total_price = f"R$ {total_price:,.2f}"

    col1.metric("Total Price", total_price)

    # create a metric card to display the total products sold
    total_products_sold = filtered_df.select("total_products_sold").collect()[0][0]
    col2.metric("Total Products Sold", total_products_sold)

    # create a metric card to count the unique combinations of user_id and product_id in df_events
    count_events = unique_events.filter(col("store_id") == store_id).count()
    col3.metric("Unique Events", count_events)

    # calulate the top 5 products with more events
    top_products = unique_events.groupBy("product_id").count().orderBy(col("count").desc()).limit(5)

    # substitue product_id por description
    top_products = top_products.join(df_products, "product_id").select("description", "count")
    
    col4.subheader("Top 5 Products with More Events")
    col4.write(top_products.toPandas())

    # filter count_events to get only the ones that resulted in a purchase
    unique_purchases = df_orders.groupBy("store_id", "user_id", "product_id").count()

    # Renomear colunas para evitar ambiguidade
    unique_events_renamed = unique_events.select(
        col("store_id").alias("events_store_id"),
        col("user_id").alias("events_user_id"),
        col("product_id").alias("events_product_id"),
        col("count").alias("events_count")
    )

    count_events_purchase = unique_events_renamed.join(
        unique_purchases,
        (unique_events_renamed.events_user_id == unique_purchases.user_id) &
        (unique_events_renamed.events_product_id == unique_purchases.product_id)
    ).select(unique_events_renamed["*"])

    # calculate median of events_count
    median_events_count = count_events_purchase.approxQuantile("events_count", [0.5], 0.001)[0]

    col5.metric("Median Events Count", median_events_count)

else:
    st.write("Overview of all stores")

    # create columns
    col1, col2, col3 = st.columns(3)
    col4, col5, col6 = st.columns(3)

    # create a metric card to display the overall total price of products sold
    overall_total_price = total_products_sold.groupBy().sum("total_price").collect()[0][0]

    # format the overall total price
    overall_total_price = f"R$ {overall_total_price:,.2f}"

    col1.metric("Overall Total Price", overall_total_price)

    # create a metric card to display the overall total products sold
    overall_total_products_sold = total_products_sold.groupBy().sum("total_products_sold").collect()[0][0]
    col2.metric("Overall Total Products Sold", overall_total_products_sold)

    # create a metric card to count the overall unique combinations of user_id and product_id in df_events
    count_events = unique_events.count()
    col3.metric("Overall Unique Events", count_events)

    # calulate the overall top 5 products with more events
    top_products = unique_events.groupBy("product_id").count().orderBy(col("count").desc()).limit(5)

    # substitue product_id por description
    top_products = top_products.join(df_products, "product_id").select("description", "count")

    col4.subheader("Overall Top 5 Products with More Events")
    col4.write(top_products.toPandas())

    # filter count_events to get only the ones that resulted in a purchase
    unique_purchases = df_orders.groupBy("store_id", "user_id", "product_id").count()

    # Renomear colunas para evitar ambiguidade
    unique_events_renamed = unique_events.select(
        col("store_id").alias("events_store_id"),
        col("user_id").alias("events_user_id"),
        col("product_id").alias("events_product_id"),
        col("count").alias("events_count")
    )

    count_events_purchase = unique_events_renamed.join(
        unique_purchases,
        (unique_events_renamed.events_user_id == unique_purchases.user_id) &
        (unique_events_renamed.events_product_id == unique_purchases.product_id)
    ).select(unique_events_renamed["*"])

    # calculate median of events_count
    median_events_count = count_events_purchase.approxQuantile("events_count", [0.5], 0.001)[0]

    col5.metric("Overall Median Events Count", median_events_count)