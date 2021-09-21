package ca.mcit.hdfs.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HadoopClient {

  val hdfsStagingDir: String = "/user/bdsf2001/manik/project9_2"
  val hadoopConfDir: String = "/Users/manikhossain/opt/hadoop-2.7.7/etc/cloudera"

  val conf = new Configuration()
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fileSystem: FileSystem = FileSystem.get(conf)
}
