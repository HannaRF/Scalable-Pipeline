import time
import datetime
import numpy as np  
import pandas as pd 
import plotly.express as px  
import streamlit as st
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, to_date, max as spark_max
from streamlit_option_menu import option_menu
from pyspark.sql.functions import col, current_timestamp, to_timestamp

# Set Hadoop home directory
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['hadoop.home.dir'] = 'C:\\hadoop'

driver = "fastdelivery/sqlite-jdbc-3.46.0.0.jar"

# initialize a Spark session for sql operations
spark = SparkSession.builder \
        .appName("quoteUpdater") \
        .config("spark.driver.extraClassPath", driver) \
        .getOrCreate()

# near real-time / live feed simulation
while True:

    # Read the quote table from the database
    quotes = spark.read.format("jdbc").options(
            url="jdbc:sqlite:fastdelivery/fastdelivery.db",
            dbtable="quote"
        ).load()

    # Read the store table from the database
    stores = spark.read.format("jdbc").options(
            url="jdbc:sqlite:fastdelivery/fastdelivery.db",
            dbtable="store"
        ).load()

    # Read the store table from the database
    products = spark.read.format("jdbc").options(
            url="jdbc:sqlite:fastdelivery/fastdelivery.db",
            dbtable="product"
        ).load()

    one_hour_ago = datetime.datetime.now() - datetime.timedelta(hours=1)

    quotes = quotes.withColumn("creation_date", col("creation_date").cast("timestamp"))

    quotes.select("creation_date").show()

    # quotes_hour = quotes.filter(col("creation_date") >= one_hour_ago)
    quotes_hour = quotes

    orders_hour = quotes_hour.filter(col("status") == "created")
    quotes_hour.filter(col("status") != "created")

    # numero de pedidos por minuto na ultima hora
    num_pedidos_min = orders_hour.groupBy("creation_date").count()
    num_pedidos_min = num_pedidos_min
    # num_pedidos_min = num_pedidos_min / 60

    print(num_pedidos_min)


    # numero de orçamentos por minuto na ultima hora por estado
    num_orcamentos_min = quotes_hour.groupBy("creation_date", "status").sum("count")
    num_orcamentos_min = num_orcamentos_min.agg({"sum(count)": "sum"}).collect()[0][0]
    num_orcamentos_min = num_orcamentos_min / 60

    # top 5 regiões com mais pedidos na ultima hora
    top5_regioes = orders_hour.groupBy("store_id").count().orderBy(col("count").desc()).limit(5)
    top5_regioes = top5_regioes.join(stores, "store_id").select("neighborhood", "count")

    quotes_hour.show()
    products.show()

    # tabela com 10 produtos mais em falta na ultima hora
    aux = products.select("product_id", "name")
    products_lack = quotes_hour.join(aux, "product_id").select("product_id", "quantity")
    top10productslack = products_lack.filter(col("quantity") <= 0).groupBy("product_id").count().orderBy(col("count").desc()).limit(10)

    # ------------------------------------------------------------------------------------------------------

    st.set_page_config(
        page_title="Real-Time Data Science Dashboard",
        page_icon="✅",
        layout="wide",
    )

    # create streamlit interface
    st.title("Real-Time Data Science Dashboard")
    st.write("Welcome to the Real-Time Data Science Dashboard! Here you can explore the latest data from your data sources.")

    col_1, col_2, col_3, col_4 = st.columns(4)

    col_1.metric("Orders per Minute", num_pedidos_min)
    col_2.metric("Quotes per Minute", num_orcamentos_min)


    col_3.subheader("Overall Top 5 Regions with More Orders")
    col_3.write(top5_regioes.toPandas())

    col_4.subheader("Top 10 Products with More Lack")
    col_4.write(top10productslack.toPandas())

    store_ids = sorted([row["store_id"] for row in products.select("store_id").distinct().collect()])
    store_id = st.selectbox("Select a store ID", store_ids, index=None)

    if store_id:
        st.write(f"Overview of store {store_id}")

        # create columns
        col1, col2, col3, col4 = st.columns(4)
        # col4, col5, col6 = st.columns(3)

        quotes_filtered = quotes.filter(col("store_id") == store_id)
        products_filtered = products.filter(col("store_id") == store_id)

        # numero total de orcamentos solicitados
        num_quotes = quotes_filtered.count()
        col1.metric("Total Quotes", num_quotes)

        # numero total de orcamentos aprovados
        num_quotes_approved = quotes_filtered.filter(col("status") == "confirmed").count()
        col2.metric("Total Quotes Approved", num_quotes_approved)

        # numero total de orcamentos cancelados ou rejeitados
        num_quotes_cancelled = quotes_filtered.filter(col("status") == "cancelled" or col("status") == "refused").count()
        col3.metric("Total Quotes Cancelled", num_quotes_cancelled)

        # valor total faturado
        total_price = quotes_filtered.select("total_price").collect()[0][0]
        total_price = f"R$ {total_price:,.2f}"
        col4.metric("Total Price", total_price)


    else:
        st.write("Overview of all stores")

        # create columns
        col1, col2, col3, col4 = st.columns(4)
        # col4, col5, col6 = st.columns(3)

        # numero total de orcamentos solicitados
        num_quotes = quotes.count()
        col1.metric("Total Quotes", num_quotes)

        # numero total de orcamentos aprovados
        num_quotes_approved = quotes.filter(col("status") == "confirmed").count()
        col2.metric("Total Quotes Approved", num_quotes_approved)

        # numero total de orcamentos cancelados ou rejeitados
        num_quotes_cancelled = quotes.filter(col("status") == "cancelled" or col("status") == "refused").count()
        col3.metric("Total Quotes Cancelled", num_quotes_cancelled)

        # valor total faturado
        total_price = quotes.select("total_price").collect()[0][0]
        total_price = f"R$ {total_price:,.2f}"
        col4.metric("Total Price", total_price)


    time.sleep(5)