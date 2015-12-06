package it.nerdammer.spark.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf

case class HBaseSparkConf (
  hbaseHost: String = HBaseSparkConf.DefaultHBaseHost,
  hbaseSitePath: String = null,
  hbaseRootDir: String = HBaseSparkConf.DefaultHBaseRootDir) extends Serializable {

  def createHadoopBaseConfig() = {
    val conf = HBaseConfiguration.create

    conf.setBoolean("hbase.cluster.distributed", true)
    conf.set("hbase.rootdir", hbaseRootDir)
    conf.set("hbase.zookeeper.quorum", hbaseHost)

    if (hbaseSitePath != null) conf.addResource(new Path(hbaseSitePath))

    conf
  }
}

object HBaseSparkConf extends Serializable {
  val DefaultHBaseHost = "localhost"
  val DefaultHBaseRootDir = "/hbase"

  def fromSparkConf(conf: SparkConf) = {
    HBaseSparkConf(
      hbaseHost = conf.get("spark.hbase.host", DefaultHBaseHost),
      hbaseSitePath = conf.get("spark.hbase.site", null),
      hbaseRootDir = conf.get("spark.hbase.root.dir", DefaultHBaseRootDir)
    )
  }
}
