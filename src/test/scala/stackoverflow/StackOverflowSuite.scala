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

    val result = testObject.clusterVector(means, vectored, 50000)
    val resultList = result.collect().toList.sortWith((a1, a2) => a1._1 < a2._1) //sort by cluster Java cluster == 0, Scala cluster == 4
    val java1 = resultList(0)

    assert(java1._1 == 1)
    assert(java1._2._1 == 1) //count should be all ones
    assert(java1._2._2._1 == 1) //lang index for Java should be 1
    assert(java1._2._2._2 == 50)

    val java2 = resultList(1)

    assert(java2._1 == 1)
    assert(java2._2._1 == 1) //count should be all ones
    assert(java2._2._2._1 == 1) //lang index for Java should be 1
    assert(java2._2._2._2 == 90)

    val scala1 = resultList(2)

    assert(scala1._1 == 4)
    assert(scala1._2._1 == 1) //count should be all ones
    assert(scala1._2._2._1 == 4) //lang index for Scala should be 1
    assert(scala1._2._2._2 == 150)


    resultList.foreach(f => println(f))
  }


  test("test calculateTotals") {
    val means = Array((0, 10), (50000, 30), (100000, 40), (150000, 100), (200000, 100))
    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings((sc.parallelize(linesSeq)))
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)
    val vectored = testObject.vectorPostings(scored)
    vectored.foreach(f => println(f))
    val result = testObject.calculateTotals(means, vectored, 50000)
    val resultList = result.collect().toList.sortWith((a1, a2) => a1._1 < a2._1) //sort by cluster index
    resultList.foreach(f => println(f))

    val javaCluster = resultList(0)
    assert(javaCluster._1 == 1)
    assert(javaCluster._2._1 == 2) //there are 2 postings in java cluster
    assert(javaCluster._2._2._1 == 2) //java index is 1
    assert(javaCluster._2._2._2 == 140) //java postings scores 90, 50

    val scalaCluster = resultList(1)
    assert(scalaCluster._1 == 4)
    assert(scalaCluster._2._1 == 1) //there is postings in scala cluster
    assert(scalaCluster._2._2._1 == 4) //scala index is 4
    assert(scalaCluster._2._2._2 == 150) //highest score is 150
  }

  test("test calculateAverages") {
    val means = Array((0, 10), (50000, 30), (100000, 40), (150000, 100), (200000, 100))
    val lines = sc.parallelize(linesSeq)
    val grouped = groupedPostings((sc.parallelize(linesSeq)))
    val scored: RDD[(Posting, Int)] = scoredPostings(grouped).sortBy(p => p._1.id)
    val vectored = testObject.vectorPostings(scored)
    vectored.foreach(f => println(f))
    val totalled: RDD[(Int, (Int, (Int, Int)))] = testObject.calculateTotals(means, vectored, 50000)
    val resultList = testObject.calculateAverages(totalled).collect().toList.sortWith((a1, a2) => a1._1 < a2._1)

    assert(resultList(0)._1 == 1)
    assert(resultList(0)._2._1 == 1)
    assert(resultList(0)._2._2 == 70)

    assert(resultList(1)._1 == 4)
    assert(resultList(1)._2._1 == 4)
    assert(resultList(1)._2._2 == 150)

  }

  test("test update means") {
    val mean = Array((1, 10), (2, 20), (3, 30))
    testObject.updateMean(List((2, (200, 250)), (1, (300, 350))), mean)

    val item0 = mean(0)
    assert(item0._1 == 1 && item0._2 == 10)

    val item1 = mean(1)
    assert(item1._1 == 300 && item1._2 == 350)

    val item2 = mean(2)
    assert(item2._1 == 200 && item2._2 == 250)

  }

  val vectorSeq: Seq[(Int, (Int, Int))] = Seq(
    (1, (50000, 50)),
    (1, (50000, 60)),
    (1, (0, 90)),
    (1, (0, 70)),
    (1, (0, 60)),
    (2, (100000, 70)),
    (2, (100000, 80)),
    (2, (200000, 30)),
    (3, (200000, 9))
  )

  val vectors:RDD[(Int, (Int, Int))] = sc.parallelize(vectorSeq)

  val pythonLangIndex = 0
  val javaLangIndex = 1
  val scalaLangIndex = 4

  test("test calculateLangCountArrayMap") {

    val result = testObject.calculateLangCountArrayMap(vectors).collect().toList

    assert(result.size == 9)
    val result1: (Int, (Array[Int], ListBuffer[Int], Int)) = result(0)
    assert(result1._1 == 1)
    assert(result1._2._1(pythonLangIndex) == 0)
    assert(result1._2._1(javaLangIndex) == 1)
    assert(result1._2._2(0) == 50)
    assert(result1._2._3 == 50000)

    val result2 = result(8)
    assert(result2._1 == 3)
    assert(result1._2._1(pythonLangIndex) == 0)
    assert(result2._2._1(scalaLangIndex) == 1)
    assert(result2._2._2(0) == 9)
    assert(result2._2._3 == 200000)

  }

  test("test aggregateLangCount") {
    val aggregateLangCount1 = testObject.aggregateLangCount(testObject.calculateLangCountArrayMap(vectors))
    val result = aggregateLangCount1.collect().toList

    assert(result.size == 3)

    val result1 = result(0)
    assert(result1._1 == 1)
    assert(result1._2._1(pythonLangIndex) == 3)
    assert(result1._2._1(javaLangIndex) == 2)
    assert(result1._2._2.size == 5)
    assert(result1._2._3 == 50000)

    val median = testObject.calculateMedian(aggregateLangCount1)
    val medianResult = median.collect().toList
    assert(medianResult(0)._1 == 1)
    assert(medianResult(0)._2._1 == "Python")
    assert(medianResult(0)._2._2 == .60)
    assert(medianResult(0)._2._3 == 5)
    assert(medianResult(0)._2._4 == 60)

    assert(medianResult(1)._1 == 2)
    assert(medianResult(0)._2._1 == "Python")
    assert(medianResult(0)._2._2 == .60)
    assert(medianResult(0)._2._3 == 5)
    assert(medianResult(0)._2._4 == 60)
  }
}
