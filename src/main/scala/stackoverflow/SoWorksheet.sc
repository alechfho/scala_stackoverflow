import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import stackoverflow.Posting
import stackoverflow.StackOverflow.sc

import annotation.tailrec
import scala.reflect.ClassTag


val lines = sc.textFile("/Users/alech/coursera-workspace/stackoverflow/src/main/resources/stackoverflow.csv")
val postings = lines.map(line => {
  val arr = line.split(",")
  Posting(postingType =    arr(0).toInt,
    id =             arr(1).toInt,
    acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
    parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
    score =          arr(4).toInt,
    tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
})
val answers = postings.filter(p => p.postingType == 2).filter(p => p.parentId.isDefined).map(p => (p.parentId.get, p));
val questions = postings.filter(p => p.postingType == 1).map(p => (p.id, p))

val questionsAnswers = questions.leftOuterJoin(answers)

