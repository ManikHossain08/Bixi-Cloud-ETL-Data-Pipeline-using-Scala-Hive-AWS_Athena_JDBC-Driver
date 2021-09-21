package ca.mcit.hdfs.hive

import ca.amazon.s3.athena.AthenaManager
import io.circe._
import io.circe.generic.semiauto._
import io.circe.parser._
import org.apache.hadoop.fs.Path

import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

object EntryPoint extends App with HiveClient with HadoopClient {

  downloadJsonFiles("https://gbfs.velobixi.com/gbfs/en/system_information.json",
    "./data/system_information.json")
  downloadJsonFiles("https://gbfs.velobixi.com/gbfs/en/station_information.json",
    "./data/station_information.json")

  if (fileSystem.exists(new Path(hdfsStagingDir))) {
    fileSystem.delete(new Path(hdfsStagingDir), true)
  }

  implicit val eightStationServices: Decoder[EightStationServices] = deriveDecoder
  implicit val stations: Decoder[Stations] = deriveDecoder
  implicit val allStations: Decoder[StationList] = deriveDecoder
  implicit val stationInfo: Decoder[StationInfo] = deriveDecoder
  val inStreamStation = Source.fromFile("./data/station_information.json")
  val inLineStation = try inStreamStation.mkString finally inStreamStation.close()

  decode[StationInfo](inLineStation) match {
    case Left(error) =>
      println(s"Unable to parse the json file: $error")
    case Right(json) =>
      val outFile = fileSystem.create(new Path(s"$hdfsStagingDir/stationInformation/stationInformation.csv"))
      val bufferedOutFile = new BufferedWriter(new FileWriter(new File("./data/stationInformation.csv")))
      outFile.writeBytes(Stations.header)
      bufferedOutFile.write(Stations.header)
      json.data.stations
        .map(row => Stations.toCsv(row))
        .foreach { line =>
          outFile.writeBytes(line)
          bufferedOutFile.write(line)
        }
      outFile.close()
      bufferedOutFile.close()

      AthenaManager.loadFileToS3Bucket(
        "./data/stationInformation.csv",
        "stationInformation.csv",
        "bixistation"
      )
  }

  implicit val systems: Decoder[System] = deriveDecoder
  implicit val systemInfo: Decoder[SystemInfo] = deriveDecoder
  val inStreamSystem = Source.fromFile("./data/system_information.json")
  val inLineSystem = try inStreamSystem.mkString finally inStreamSystem.close()

  decode[SystemInfo](inLineSystem) match {
    case Left(error) =>
      println(s"Unable to parse the json file: $error")
    case Right(json) =>
      val outFile = fileSystem.create(new Path(s"$hdfsStagingDir/systemInformation/systemInformation.csv"))
      val bufferedOutFile = new BufferedWriter(new FileWriter(new File("./data/systemInformation.csv")))
      outFile.writeBytes(System.header)
      bufferedOutFile.write(System.header)
      outFile.writeBytes(System.toCsv(json.data))
      bufferedOutFile.write(System.toCsv(json.data))
      outFile.close()
      bufferedOutFile.close()

      AthenaManager.loadFileToS3Bucket(
        "./data/systemInformation.csv",
        "systemInformation.csv",
        "bixisystem"
      )
  }

  HiveManager.createHiveTables()
  val enrichedStationSQL =
    s"""insert into station_information
       | select b.system_id, b.timezone, a.station_id, b.name, a.short_name, a.lat, a.lon, a.capacity
       | from  ext_station_information a
       | cross join ext_system_information b
       | """.stripMargin
  stmt.executeUpdate(enrichedStationSQL)
  stmt.close()
  connection.close()

  AthenaManager.SQLQueryOnAthena()

  def downloadJsonFiles(jsonURL: String, fileSavePath: String): Unit = {
    val jsonDataBuffered = Source.fromURL(jsonURL)
    val strJsonData = jsonDataBuffered.mkString
    val outFile = new BufferedWriter(new FileWriter(new File(fileSavePath)))
    outFile.write(strJsonData)
    outFile.close()
    jsonDataBuffered.close()
  }
}
