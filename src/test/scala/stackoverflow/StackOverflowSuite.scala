package stackoverflow

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

import stackoverflow.StackOverflow._

import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
         "Python", "Java", "Ruby", "CSS", "Scala")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  val linesSeq = Seq(
    Posting(1, 6, None, None, 50, Some("CSS")),
    Posting(1, 7, None, None, 60, Some("Java")),
    Posting(1, 8, None, None, 10, Some("Python")),
    Posting(1, 9, Option(29), None, 80, Some("Scala")),
    Posting(1, 10, Option(20), None, 150, Some("Java")),
    Posting(1, 40, None, None, 150, Some("Java")),
    Posting(2, 16, None, None, 50, Some("CSS")),
    Posting(2, 17, None, Option(10), 60, Some("Java")),
    Posting(2, 18, None, None, 10, Some("Python")),
    Posting(2, 19, None, Option(9), 80, Some("Scala")),
    Posting(2, 20, None, Option(10), 90, Some("Java")),
    Posting(2, 26, None, Option(7), 50, Some("Java")),
    Posting(2, 27, None, None, 60, Some("Java")),
    Posting(2, 28, None, None, 10, Some("Python")),
    Posting(2, 29, None, Option(9), 70, Some("Scala")),
    Posting(2, 30, None, Option(9), 150, Some("Scala")
    ))


  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("test grouping") {
    import StackOverflow._
    val lines: RDD[Posting] = sc.parallelize(linesSeq)
    val grouped = groupedPostings(lines)
    assert(grouped.count() == 3)
    val result: Array[(Int, Iterable[(Posting, Posting)])] = grouped.sortByKey().collect()
    val g1: (Int, Iterable[(Posting, Posting)]) = result {
      0
    }
    assert(g1._1 == 7)
    assert(g1._2.size == 1)
    val g2: (Int, Iterable[(Posting, Posting)]) = result {
      1
    }
    assert(g2._1 == 9)
    assert(g2._2.size == 3)
  }

  test("test scoredPostings") {
    import StackOverflow._
    val scalaPostingId = 9
    val javaPostingId = 10

    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings(lines)
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)

    val scalaPosting = scored.filter(p => p._1.id == scalaPostingId).collect(){0}
    assert(scalaPosting._1.id == 9 && scalaPosting._2 == 150)
    val javaPosting = scored.filter(p => p._1.id == javaPostingId).collect(){0}
    assert(javaPosting._1.id == 10 && javaPosting._2 == 90)
  }



  test("test vectorize scoredPostings") {
    import StackOverflow._

    val scalaPostingId = 9
    val javaPostingId = 10

    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings((sc.parallelize(linesSeq)))
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)
    scored.collect().foreach(s => println(s))
    val vectored = vectorPostings(scored)
    val negative = vectored.filter(v => v._1 < 0)
    assert(negative.collect().size == 0)
    val result = vectored.collect().toList
    val javaPosting = result{0}
    val javaIndex = 1
    val scalaIndex = 10
    assert(javaPosting._1 == (50000 * javaIndex) && javaPosting._2 == 50)
    val scalaPosting = result{1}
    assert(scalaPosting._1 == (50000 * scalaIndex) && scalaPosting._2 == 150)
    val javaPosting2 = result{2}
    assert(javaPosting2._1 == (50000 * javaIndex) && javaPosting2._2 == 90)
  }

  test("test cluster vector") {
    val means = Array((0, 10), (50000, 30), (100000, 40), (150000, 100), (200000, 100))
    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings((sc.parallelize(linesSeq)))
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)
    val vectored = testObject.vectorPostings(scored)

    val result = testObject.clusterVector(means, vectored)
    val resultList = result.collect().toList.sortWith((a1, a2) => a1._1 < a2._1) //sort by cluster Java cluster == 0, Scala cluster == 4
    val java1 = resultList(0)

    assert(java1._1 == 1)
    assert(java1._2._1 == 50000)
    assert(java1._2._2 == 50)

    val java2 = resultList(1)

    assert(java2._1 == 1)
    assert(java2._2._1 == 50000)
    assert(java2._2._2 == 90)

    val scala1 = resultList(2)

    assert(scala1._1 == 4)
    assert(scala1._2._1 == 200000)
    assert(scala1._2._2 == 150)

  }

  test("test kmeans") {
    val means = Array((0, 10), (50000, 30), (100000, 40), (150000, 100), (200000, 100))
    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings((sc.parallelize(linesSeq)))
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)
    val vectored = testObject.vectorPostings(scored)

    testObject.kmeans(means, vectored, 1, false)

  }

  val closetSeq: Seq[(Int, (Int, Int))] = Seq(
    (1, (50000, 50)),
    (1, (50000, 60)),
    (1, (50000, 60)),
    (1, (0, 90)),
    (1, (0, 70)),
    (1, (0, 60)),
    (1, (0, 60)),
    (2, (100000, 70)),
    (2, (100000, 80)),
    (2, (200000, 30)),
    (3, (200000, 9))
  )

  val vectors:RDD[(Int, (Int, Int))] = sc.parallelize(closetSeq)

  val pythonLangIndex = 0
  val javaLangIndex = 1
  val scalaLangIndex = 4

  test("test calculateMedian") {
    val median = testObject.calculateMedian(vectors.groupByKey()).collect().toList
    val median1 = median(0)
    assert(median1._2._1 == "Python")
    assert(median1._2._2 == 57.142857142857146)
    assert(median1._2._3 == 7)
    assert(median1._2._4 == 65)

  }

}
