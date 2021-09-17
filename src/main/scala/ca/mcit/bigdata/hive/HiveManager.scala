package ca.mcit.bigdata.hive

object HiveManager extends HiveClient {

  private val stagingDir = "/user/bdsf2001/manik/project9_2"

  def createHiveTables(): Unit = {

    dropExistingTable("ext_station_information")
    stmt.executeUpdate(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ext_station_information (
         |    station_id STRING,
         |    lon DOUBLE,
         |    lat DOUBLE,
         |    is_charging BOOLEAN,
         |    external_id STRING,
         |    capacity Int,
         |    eightd_has_key_dispenser BOOLEAN,
         |    electric_bike_surcharge_waiver BOOLEAN,
         |    has_kiosk BOOLEAN,
         |    rental_methods String,
         |    short_name String,
         |    eightd_station_services String
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | LOCATION '$stagingDir/stationInformation/'
         | TBLPROPERTIES (
         | 'skip.header.line.count' = '1' ,
         | 'serialization.null.format' = ''
         | )""".stripMargin)

    dropExistingTable("ext_trips")
    stmt.executeUpdate(
      s"""CREATE EXTERNAL TABLE IF NOT EXISTS ext_system_information (
         |    email STRING,
         |    language String,
         |    license_url String,
         |    name String,
         |    operator STRING,
         |    phone_number String,
         |    purchase_url String,
         |    short_name String,
         |    start_date String,
         |    system_id String,
         |    timezone String,
         |    url String
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY ','
         | LOCATION '$stagingDir/systemInformation/'
         | TBLPROPERTIES (
         | 'skip.header.line.count' = '1' ,
         | 'serialization.null.format' = ''
         | )""".stripMargin)

    dropExistingTable("station_information")
    stmt.executeUpdate(
      s"""CREATE TABLE IF NOT EXISTS station_information (
         |    system_id STRING,
         |    timezone STRING,
         |    station_id INT,
         |    name STRING,
         |    short_name STRING,
         |    lat DOUBLE,
         |    lon DOUBLE,
         |    capacity INT
         | )
         | STORED as PARQUET
         | TBLPROPERTIES (
         | 'parquet.compression'='GZIP'
         | )""".stripMargin)

    stmt.close()
  }

  def dropExistingTable(tableName: String): Unit = {
    val dropTableSQL = s"DROP TABLE IF EXISTS $tableName"
    stmt.execute(dropTableSQL)
  }
}
