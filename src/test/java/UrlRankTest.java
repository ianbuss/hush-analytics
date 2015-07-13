import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.junit.Before;

public class UrlRankTest extends LocalJavaSparkContext {

  @Before
  public void setup() {
    SparkConf conf = new SparkConf().setAppName("URL Ranker").setMaster("local[2]");
    sc = new JavaSparkContext(conf);
  }

  @Test
  public void testRanker() {
    UrlRank ranker = new UrlRank(sc);
    ranker.run();
  }

}
