import io.netty.util.internal.logging.{InternalLoggerFactory, Slf4JLoggerFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.xerial.snappy.SnappyLoader

/** Manages a local `sc` {@link SparkContext} variable, correctly stopping it after each test. */
trait LocalSparkContext extends BeforeAndAfterEach with BeforeAndAfterAll {
  self: Suite =>
  @transient var sc: SparkContext = _

  override def beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory())

    // SnappyLoader workaround for JDK7 on OSX for https://github.com/xerial/snappy-java/issues/6
    if (System.getProperty("os.name").contains("Mac"))
      System.setProperty(SnappyLoader.KEY_SNAPPY_LIB_NAME, "libsnappyjava.jnilib")

    super.beforeAll()
  }

  override def afterEach() {
    LocalSparkContext.stop(sc)
    sc = null
    super.afterEach()
  }

  override def beforeEach() {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("LocalSparkTest")
    sc = new SparkContext(conf)
  }
}

object LocalSparkContext {
  def stop(sc: SparkContext) {
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }
}
