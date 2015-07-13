import org.scalatest.FunSuite

class UrlRankScalaTest extends FunSuite with LocalSparkContext {

  test("run urlrank") {
    val ranker = new UrlRankScala(sc)
    ranker.run()
  }

}
