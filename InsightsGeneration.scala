// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

val flightsAirlines = flights.as("A").join(airlines.as("B"), $"A.AIRLINE" === $"B.airline_cd","leftouter").drop($"B.airline_cd")

val flightsAirlinesAirport = flightsAirlines.as("A").join(airports.as("B"), $"A.ORIGIN_AIRPORT" === $"B.airport_cd").drop($"B.airport_cd")

//Report 1 Total number of flights by airline and airport on a monthly basis
val result = flightsAirlinesAirport.groupBy($"flight_year_month", $"airline_name", $"airport_name").count

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
result.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "flights_airline_airport").save()



// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

//Report 2 On time percentage of each airline for the year 2015
val flightsAirlines = flights.filter($"YEAR" === 2015).as("A").join(airlines.as("B"), $"A.AIRLINE" === $"B.airline_cd","leftouter").drop($"B.airline_cd")

//Total airline count
val totalAirlineCount = flightsAirlines.groupBy($"airline_name").count

//Getting Airlines which are on time
val onTimeRawCount = flightsAirlines.select($"airline_name", when($"DEPARTURE_DELAY" <= 0, 1).otherwise(0).as("on_time_raw_count"))

//Creating group of airlines with their ontime counts
val onTimeCount = onTimeRawCount.groupBy($"airline_name").agg(sum($"on_time_raw_count").as("on_time_count"))

//Calculating ontime percentage
val result = totalAirlineCount.join(onTimeCount, Seq("airline_name")).select($"airline_name", (($"on_time_count"/$"count")*100).cast(DecimalType(22,2)).as("on_time_pct"))

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
result.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "airline_on_time_pct").save()


// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

val flightsAirlines = flights.as("A").join(airlines.as("B"), $"A.AIRLINE" === $"B.airline_cd","leftouter").drop($"B.airline_cd")

//Report 3 Airlines with the largest number of delays

//Getting flights which are delayed
val delayRawCount = flightsAirlines.select($"airline_name", when($"DEPARTURE_DELAY" > 0, 1).otherwise(0).as("delay_raw_count"))

//Getting delayed counts
val delayCount = delayRawCount.groupBy($"airline_name").agg(sum($"delay_raw_count").as("delay_count"))

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
delayCount.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "airlines_delay_count").save()




// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

val flightsAirlines = flights.as("A").join(airlines.as("B"), $"A.AIRLINE" === $"B.airline_cd","leftouter").drop($"B.airline_cd")

val flightsAirlinesAirport = flightsAirlines.as("A").join(airports.as("B"), $"A.ORIGIN_AIRPORT" === $"B.airport_cd").drop($"B.airport_cd")

// Report 4 Cancellation reasons by airport
//Getting flights which are cancelled and generating new column cancel_reason
val cancelReasonAirport = flightsAirlinesAirport.filter($"CANCELLED" === 1).select($"airport_name", when($"CANCELLATION_REASON" === "A", "Airline/Carrier").when($"CANCELLATION_REASON" === "B", "Weather").when($"CANCELLATION_REASON" === "C", "National Air System").when($"CANCELLATION_REASON" === "D", "Security").otherwise("Other").as("cancel_reason"))

//Getting ditinct reason of cancel by airport name
val result = cancelReasonAirport.select($"airport_name", $"cancel_reason").distinct

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
result.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "airport_can_reason").save()


// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

//Report 5 Delay reasons by airport

//Getting flights records which are delayed and then transposing row data into columns
val delayReason = flights.filter($"DEPARTURE_DELAY" > 0 and $"AIR_SYSTEM_DELAY" > 0).select($"ORIGIN_AIRPORT", lit("AIR_SYSTEM_DELAY").as("delay_reason")).union(flights.filter($"DEPARTURE_DELAY" > 0 and $"SECURITY_DELAY" > 0).select($"ORIGIN_AIRPORT", lit("SECURITY_DELAY").as("delay_reason"))).union(flights.filter($"DEPARTURE_DELAY" > 0 and $"AIRLINE_DELAY" > 0).select($"ORIGIN_AIRPORT", lit("AIRLINE_DELAY").as("delay_reason"))).union(flights.filter($"DEPARTURE_DELAY" > 0 and $"LATE_AIRCRAFT_DELAY" > 0).select($"ORIGIN_AIRPORT", lit("LATE_AIRCRAFT_DELAY").as("delay_reason"))).union(flights.filter($"DEPARTURE_DELAY" > 0 and $"WEATHER_DELAY" > 0).select($"ORIGIN_AIRPORT", lit("WEATHER_DELAY").as("delay_reason"))).select($"ORIGIN_AIRPORT", $"delay_reason").distinct

//Attaching airport name
val result = delayReason.join(airports, $"ORIGIN_AIRPORT" === $"airport_cd", "leftouter").select($"airport_name", $"delay_reason")

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
result.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "airport_delay_reason").save()




// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType

val flights = spark.table("default.flights")
val airlines = spark.table("default.airlines").select($"IATA_CODE".as("airline_cd"),$"AIRLINE".as("airline_name"))
val airports = spark.table("default.airports").select($"IATA_CODE".as("airport_cd"),$"AIRPORT".as("airport_name"))

//Report 6 Airline with the most unique routes
//Get count of all the routes by airline and then filter only those route which have only 1 route and then get sum of route
val result = flights.select($"AIRLINE", $"ORIGIN_AIRPORT", $"DESTINATION_AIRPORT").groupBy($"AIRLINE", $"ORIGIN_AIRPORT", $"DESTINATION_AIRPORT").count.filter($"count" === 1).groupBy($"AIRLINE").agg(sum($"count").as("unique_route")).as("A").join(airlines.as("B"), $"A.AIRLINE" === $"B.airline_cd","leftouter").drop($"B.airline_cd")

//Getting credentials from table. Ideally it should come from secret Manager
val user = spark.table("default.credentials_1_csv").select("user_name").first
val password = spark.table("default.credentials_1_csv").select("password").first

//Snowflake Connection String 
val options = Map(
  "sfUrl" -> "hxa65217.snowflakecomputing.com",
  "sfUser" -> user.getString(0),
  "sfPassword" -> password.getString(0),
  "sfDatabase" -> "USER_VIJIT",
  "sfSchema" -> "LEARNING",
  "sfWarehouse" -> "INTERVIEW_WH"
)

//Saving Report in Snowflake
result.write.mode("overwrite").format("snowflake").options(options).option("dbtable", "airline_unique_route").save()

