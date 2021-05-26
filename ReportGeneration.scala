// Databricks notebook source
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Total number of flights by airline and airport on a monthly basis
val df = spark.read.format("snowflake").options(options).option("dbtable", "airline_on_time_pct").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//On time percentage of each airline for the year 2015
val df = spark.read.format("snowflake").options(options).option("dbtable", "flights_airline_airport").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Airlines with the largest number of delays
val df = spark.read.format("snowflake").options(options).option("dbtable", "airlines_delay_count").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Cancellation reasons by airport
val df = spark.read.format("snowflake").options(options).option("dbtable", "airport_can_reason").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Delay reasons by airport
val df = spark.read.format("snowflake").options(options).option("dbtable", "AIRPORT_DELAY_REASON").load()

display(df)

// COMMAND ----------

val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Airline with the most unique routes
val df = spark.read.format("snowflake").options(options).option("dbtable", "airline_unique_route").load()

display(df)
