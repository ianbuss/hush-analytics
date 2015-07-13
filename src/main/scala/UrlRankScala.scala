import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

class UrlRankScala(val sc: SparkContext) {

  def run() {
    val config = HBaseConfiguration.create()
    val hc = new HBaseContext(sc, config)

    val yesterday = UrlRankScala.DTF.print(LocalDate.now.minusDays(1))
    val yesterdayStats = (yesterday + "\u0000\u0001").getBytes
    val yesterdayBC = sc.broadcast(yesterday)
    val yesterdayRevBC = sc.broadcast(yesterday.reverse)
    val yesterdayStatsBC = sc.broadcast(yesterdayStats)

    val scan = new Scan
    scan.addColumn(UrlRankScala.DAILY_COLUMN, yesterdayStatsBC.value)
    scan.addColumn(UrlRankScala.DATA_COLUMN, UrlRankScala.URL_COLUMN)

    val dailyStats = hc.hbaseRDD(UrlRankScala.TABLE, scan, d => d).map {
      case (rk, results) => {
        val url = Bytes.toString(results.getValue(UrlRankScala.DATA_COLUMN, UrlRankScala.URL_COLUMN))
        val count = Bytes.toLong(results.getValue(UrlRankScala.DAILY_COLUMN, yesterdayStatsBC.value))
        (url, count)
      }
    }.reduceByKey(_ + _).map{ case(url, count) => (count, url) }.sortByKey().zipWithIndex()

    // Add the ranks to the URLs table
    hc.bulkPut[((Long,String), Long)](dailyStats, UrlRankScala.URLS_TABLE, d => {
      val put = new Put(d._1._2.getBytes())
      put.add(UrlRankScala.URL_RANK_COLUMN, (yesterdayBC.value + "\u0000" + (d._2 + 1)).getBytes, Bytes.toBytes(d._1._1))
      put
    }, false)

    // Add the ranks to the ranks table, keyed by reversed-day and rank
    hc.bulkPut[((Long,String), Long)](dailyStats, UrlRankScala.RANKS_TABLE, d => {
      val put = new Put((yesterdayRevBC.value + "\u0000" + (d._2 + 1)).getBytes)
      put.add(UrlRankScala.DATA_COLUMN, Bytes.toBytes(d._1._2), Bytes.toBytes(d._1._1))
      put
    }, false)
  }

}

object UrlRankScala {

  private val TABLE = "surl"
  private val RANKS_TABLE = "ranks"
  private val URLS_TABLE = "urls"
  private val DAILY_COLUMN = Bytes.toBytes("std")
  private val DATA_COLUMN = Bytes.toBytes("data")
  private val URL_COLUMN = Bytes.toBytes("url")
  private val URL_RANK_COLUMN = Bytes.toBytes("rnk")
  private val DTF = DateTimeFormat.forPattern("yyyyMMdd")

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("URL Ranker")
    val sc = new SparkContext(conf)

    val ranker = new UrlRankScala(sc)
    ranker.run()

  }

}
