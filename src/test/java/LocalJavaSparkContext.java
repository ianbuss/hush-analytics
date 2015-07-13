import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.BeforeClass;
import org.xerial.snappy.SnappyLoader;

public class LocalJavaSparkContext {

  protected JavaSparkContext sc;

  @BeforeClass
  public static void beforeAll() {
    InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());

    // SnappyLoader workaround for JDK7 on OSX for https://github.com/xerial/snappy-java/issues/6
    if (System.getProperty("os.name").contains("Mac")) {
      System.setProperty(SnappyLoader.KEY_SNAPPY_LIB_NAME, "libsnappyjava.jnilib");
    }
  }

  @After
  public void after() {
    if (sc != null) {
      sc.stop();
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port");
    sc = null;
  }

}
