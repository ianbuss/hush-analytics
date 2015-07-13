import com.cloudera.spark.hbase.JavaHBaseContext;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class UrlRank implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(UrlRank.class);

  private static final DateTimeFormatter DTF = DateTimeFormat.forPattern("yyyyMMdd");
  private static final byte[] DATA_FAMILY = Bytes.toBytes("data");
  private static final byte[] DAILY_FAMILY = Bytes.toBytes("std");
  private static final byte[] RANK_FAMILY = Bytes.toBytes("rnk");
  private static final byte[] URL = Bytes.toBytes("url");
  private static final String URLS = "urls";
  private static final String RANKS = "ranks";
  private static final String TABLE = "surl";

  protected transient JavaSparkContext sc;

  public UrlRank(JavaSparkContext sc) {
    this.sc = sc;
  }

  public void run() {

    Configuration config = HBaseConfiguration.create();
    JavaHBaseContext hc = new JavaHBaseContext(sc, config);

    String yesterday = DTF.print(LocalDate.now().minusDays(1));
    final Broadcast<String> yesterdayBC = sc.broadcast(yesterday);
    final Broadcast<String> yesterdayRevBC = sc.broadcast(StringUtils.reverse(yesterday));
    final Broadcast<String> yesterdayStatsBC = sc.broadcast(yesterday + "\u0000\u0001");

    final Scan statsScan = new Scan();
    statsScan.addColumn(DAILY_FAMILY, yesterdayStatsBC.value().getBytes());
    statsScan.addColumn(DATA_FAMILY, URL);

    JavaRDD<Tuple2<Tuple2<Long, String>, Long>> dailyStats = hc.hbaseRDD(TABLE, statsScan,
      new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<String, Result>>() {
        @Override
        public Tuple2<String, Result> call(Tuple2<ImmutableBytesWritable, Result> arg) throws Exception {
          return new Tuple2<>(
            Bytes.toString(arg._1().get()),
            arg._2()
          );
        }
      }).flatMapToPair(new PairFlatMapFunction<Tuple2<String, Result>, String, Long>() {
        @Override
        public Iterable<Tuple2<String, Long>> call(Tuple2<String, Result> arg) throws Exception {
          LOG.info("{} => {}", arg._1(), arg._2());
          List<Tuple2<String, Long>> l = new ArrayList<>();
          if (arg._2().containsColumn(DAILY_FAMILY, Bytes.toBytes(yesterdayStatsBC.value()))) {
            l.add(new Tuple2<> (
              Bytes.toString(arg._2().getValue(DATA_FAMILY, URL)),
              Bytes.toLong(arg._2().getValue(DAILY_FAMILY, Bytes.toBytes(yesterdayStatsBC.value())))
            ));
          }
          return l;
        }
      }).reduceByKey(new Function2<Long, Long, Long>() {
        @Override
        public Long call(Long a, Long b) throws Exception {
          return a + b;
        }
      }).mapToPair(new PairFunction<Tuple2<String,Long>, Long, String>() {
        @Override
        public Tuple2<Long, String> call(
          Tuple2<String, Long> arg) throws Exception {
          return new Tuple2<Long, String>(arg._2(), arg._1());
        }
      }).sortByKey().zipWithIndex().map(new Function<Tuple2<Tuple2<Long, String>, Long>, Tuple2<Tuple2<Long, String>, Long>>() {
        @Override
        public Tuple2<Tuple2<Long, String>, Long> call(
          Tuple2<Tuple2<Long, String>, Long> arg) throws Exception {
          return arg;
        }
      });

    // Add the ranks to the URLs table
    hc.bulkPut(dailyStats, URLS, new Function<Tuple2<Tuple2<Long, String>, Long>, Put>() {
      @Override
      public Put call(Tuple2<Tuple2<Long, String>, Long> arg) throws Exception {
        Put put = new Put(arg._1()._2().getBytes());
        put.add(RANK_FAMILY, (yesterdayBC.value() + "\u0000" + (arg._2() + 1)).getBytes(),
          Bytes.toBytes(arg._1()._1()));
        return put;
      }
    }, false);

    // Add the ranks to the ranks table, keyed by reversed-day and rank
    hc.bulkPut(dailyStats, RANKS, new Function<Tuple2<Tuple2<Long, String>, Long>, Put>() {
      @Override
      public Put call(Tuple2<Tuple2<Long, String>, Long> arg) throws Exception {
        Put put = new Put((yesterdayRevBC.value() + "\u0000" + (arg._2() + 1)).getBytes());
        put.add(DATA_FAMILY, Bytes.toBytes(arg._1()._2()), Bytes.toBytes(arg._1()._1()));
        return put;
      }
    }, false);

  }

  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("URL Ranker");
    JavaSparkContext sc = new JavaSparkContext(conf);

    UrlRank ranker = new UrlRank(sc);
    ranker.run();

  }

}
