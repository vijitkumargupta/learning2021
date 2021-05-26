// Databricks notebook source
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "XXXXX",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "XXXX"
)

//Total number of flights by airline and airport on a monthly basis
val df = spark.read.format("snowflake").options(options).option("dbtable", "airline_on_time_pct").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "xxxxx",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "xxxxxx"
)

//On time percentage of each airline for the year 2015
val df = spark.read.format("snowflake").options(options).option("dbtable", "flights_airline_airport").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "xxxxx",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "xxxxx"
)

//Airlines with the largest number of delays
val df = spark.read.format("snowflake").options(options).option("dbtable", "airlines_delay_count").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "xxxxx",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "xxxxx"
)

//Cancellation reasons by airport
val df = spark.read.format("snowflake").options(options).option("dbtable", "airport_can_reason").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "xxxxx",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "xxxxx"
)

//Delay reasons by airport
val df = spark.read.format("snowflake").options(options).option("dbtable", "AIRPORT_DELAY_REASON").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "xxxxx.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "xxxx",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "xxxxx"
)

//Airline with the most unique routes
val df = spark.read.format("snowflake").options(options).option("dbtable", "airline_unique_route").load()

display(df)
