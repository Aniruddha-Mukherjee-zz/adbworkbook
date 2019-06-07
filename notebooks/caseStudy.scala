// Databricks notebook source
val secret = dbutils.secrets.get(scope = "sql-credentials", key = "sqlpassword")
print(secret)

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "adbiomegasqlserverv2.database.windows.net",
  "databaseName"   -> "adbsqldatabase ",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "uff",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)

collection.createOrReplaceTempView("Customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

val query = "SELECT * FROM Locations"
val config1 = Config(Map(
	  "url"          -> "adbiomegasqlserverv2.database.windows.net",
	  "databaseName" -> "adbsqldatabase",
	  "user"         -> "uff",
	  "password"     -> secret,
	  "queryCustom"  -> query
	))

val collection1 = spark.read.sqlDB(config1)

collection1.createOrReplaceTempView("locations")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM Locations

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC WHERE L.State IN ( "AP", "TN", "KA" )

// COMMAND ----------

val dlId =     dbutils.secrets.get(scope = "trainingScope", key = "ClientId")
val dlsSecret = dbutils.secrets.get(scope = "trainingScope", key = "ClientSecret")
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> dlId,
  "fs.azure.account.oauth2.client.secret" -> dlsSecret,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@adbstorageaccount.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC 
// MAGIC ls /mnt/dlsdata/salesfiles/

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/salesfiles/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("factsales")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP VIEW Processedresults 
// MAGIC AS
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )
// MAGIC     

// COMMAND ----------

val results =  spark.sql("SELECT * FROM ProcessedResults")

results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")

// COMMAND ----------

val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)

// COMMAND ----------

// DBTITLE 1,DW and Polybase Config for Bulk Update
val blobStorage = "adbpolybase.blob.core.windows.net"
	val blobContainer = "tempdata"
	val blobAccessKey =  "LBCtzTUZ0+e2rrdcczr5WFf51V7Bd83DaGiPCJd+uKbTseF+NHYnGXMBhdjJAJAEHmM0xQmMK8g3tK2AYuw1KA=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

// DBTITLE 1,DW Configuration
//SQL Data Warehouse related settings
	val dwDatabase = "adb-warehouse"
	val dwServer = "adbiomegasqlserverv2.database.windows.net"
	val dwUser = "uff"
	val dwPass = secret
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

	spark.conf.set(
		"spark.sql.parquet.writeLegacyFormat",
		"true")

// COMMAND ----------

results.write
		.format("com.databricks.spark.sqldw")
		.option("url", sqlDwUrlSmall) 
		.option("dbtable", "ProcessedResults")
		.option("forward_spark_azure_storage_credentials","True")
		.option("tempdir", tempDir)
		.mode("overwrite")
		.save()

// COMMAND ----------

