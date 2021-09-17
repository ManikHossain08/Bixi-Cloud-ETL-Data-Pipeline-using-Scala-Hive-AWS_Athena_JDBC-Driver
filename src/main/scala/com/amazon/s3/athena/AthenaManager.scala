package com.amazon.s3.athena

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client

import java.io.File
import java.sql.DriverManager
import java.util.Properties

object AthenaManager {

  def saveFileToS3Bucket(localFilePath: String, fileName: String, s3BucketName: String): Unit = {
    val bucketName = s3BucketName
    val AWS_ACCESS_KEY = "AKIAXEDGCGQSTCYA6AUR"
    val AWS_SECRET_KEY = "NeQzmw+fnRvEMfwEK79pNP7Wdu6m46SMS0EHg+dI"

    val fileToUpload = new File(localFilePath)
    val yourAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
    amazonS3Client.createBucket(bucketName)
    amazonS3Client.putObject(bucketName, fileName, fileToUpload)
  }

  def SQLQueryOnAthena(): Unit = {
    Class.forName("com.simba.athena.jdbc.Driver")
    val athenaUrl = "jdbc:awsathena://AwsRegion=us-east-1;"
    val athenaProperties = new Properties()
    athenaProperties.put("User", "AKIAXEDGCGQSTCYA6AUR")
    athenaProperties.put("Password", "NeQzmw+fnRvEMfwEK79pNP7Wdu6m46SMS0EHg+dI")
    athenaProperties.put("S3OutputLocation", "s3://mcitoutput/output/")
    athenaProperties.put("AwsCredentialsProviderClass",
      "com.simba.athena.amazonaws.auth.PropertiesFileCredentialsProvider")
    athenaProperties.put("AwsCredentialsProviderArguments", "./data/athenaCredentials")
    athenaProperties.put("driver", "com.simba.athena.jdbc.Driver")

    val connection = DriverManager.getConnection(athenaUrl, athenaProperties)
    val stmt = connection.createStatement()

    stmt.execute("DROP TABLE if exists sampledb.ext_station_information;")
    stmt.execute(
      """CREATE TABLE IF NOT EXISTS sampledb.ext_station_information (
        |         station_id STRING,
        |         lon DOUBLE,
        |         lat DOUBLE,
        |         is_charging BOOLEAN,
        |         external_id STRING,
        |         capacity Int,
        |         eightd_has_key_dispenser BOOLEAN,
        |         electric_bike_surcharge_waiver BOOLEAN,
        |         has_kiosk BOOLEAN,
        |         rental_methods String,
        |         short_name String,
        |         eightd_station_services String
        |)
        | ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        | WITH SERDEPROPERTIES (
        |         'serialization.format' = ',',
        |         'field.delim' = ','
        |     )
        |  LOCATION 's3://bixistation/'
        |  TBLPROPERTIES ( 'has_encrypted_data'='false', 'skip.header.line.count'='1');
        |  """.stripMargin
    )

    stmt.execute("DROP TABLE if exists sampledb.ext_system_information;")
    stmt.execute(
      """CREATE TABLE IF NOT EXISTS sampledb.ext_system_information (
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
        |)
        | ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        | WITH SERDEPROPERTIES (
        |         'serialization.format' = ',',
        |         'field.delim' = ','
        |         )
        | LOCATION 's3://bixisystem/'
        | TBLPROPERTIES ( 'has_encrypted_data'='false', 'skip.header.line.count'='1');
        | """.stripMargin
    )

    stmt.execute("DROP TABLE if exists sampledb.system_information;")
    stmt.execute(
      """CREATE TABLE IF NOT EXISTS sampledb.station_information (
        |    system_id STRING,
        |    timezone STRING,
        |    station_id INT,
        |    name STRING,
        |    short_name STRING,
        |    lat DOUBLE,
        |    lon DOUBLE,
        |    capacity INT
        | )
        | STORED AS PARQUET
        | LOCATION 's3://bixistationinformation/'
        | TBLPROPERTIES ( 'has_encrypted_data'='false',
        | 'parquet.compression'='GZIP');
        | """.stripMargin
    )

    stmt.execute(
      """insert into sampledb.station_information
        | select b.system_id, b.timezone, cast(a.station_id as integer),
        | b.name, a.short_name, a.lat, a.lon, a.capacity
        | from  sampledb.ext_station_information a
        | cross join sampledb.ext_system_information b
        | """.stripMargin)

    connection.close()
  }
}
