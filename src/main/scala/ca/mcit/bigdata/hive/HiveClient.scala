package ca.mcit.bigdata.hive

import org.apache.hive.jdbc.HiveDriver
import java.sql.{Connection, DriverManager, Statement}

trait HiveClient {

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_manik;user=manik;"
  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection(connectionString)
  val stmt: Statement = connection.createStatement()
}
