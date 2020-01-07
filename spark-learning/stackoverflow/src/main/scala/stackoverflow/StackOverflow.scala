package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    // vectors.take(100).foreach(println)
    // assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000

  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType = arr(0).toInt,
        id = arr(1).toInt,
        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId = if (arr(3) == "") None else Some(arr(3).toInt),
        score = arr(4).toInt,
        tags = if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    // que RDD[(QID, Posting)]，QID作为连接question和answer的key
    val que = postings
      .filter(p => p.postingType == 1)
      .map(p => {
        (p.id, p)
      })

    // ans RDD[(QID, Posting)]，QID作为连接question和answer的key
    val ans = postings
      .filter(p => p.postingType == 2)
      .map(p => {
        (p.parentId.get, p)
      })

    // RDD[(QID, Iterable[(Question, Answer)])]
    que
      .join(ans)
      .groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
      var i = 0
      while (i < as.length) {
        val score = as(i).score
        if (score > highScore)
          highScore = score
        i += 1
      }
      highScore
    }

    // 对每个question找出其所有answer最高的分数
    grouped
      .mapValues(record => {
        var ques: Question = null
        var answer: List[Answer] = Nil
        for (elem <- record) {
          if (ques == null) {
            ques = elem._1
          }
          answer = answer :+ elem._2
        }
        (ques, answer.toArray)
      })
      .map({ case (qid, record) =>
        (record._1, answerHighScore(record._2))
      })
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    // 扩大语言特征的区分度，以index为基础，乘以系数langSpread
    scored.map(record => {
      (firstLangInTag(record._1.tags, langs).get * langSpread, record._2)
    })
      .cache()
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
      // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
      // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // val newMeans = means.clone() // you need to compute newMeans
    // 新簇中心点
    val newMeans = new Array[(Int, Int)](means.length)

    // 找到所有向量所属的簇，RDD[(Int,(Int,Int))]，key为第n个簇，value表示特征向量
    val closest = vectors.map(p => (findClosest(p, means), p)).cache()

    // 求新簇中每个簇中心的中心点
    for (idx <- 0 until kmeansKernels) {
      val vectorArray = closest.filter(_._1 == idx).map(_._2).collect()
      if (vectorArray.length != 0) {
        newMeans(idx) = averageVectors(vectorArray)
      } else {
        newMeans(idx) = means(idx)
      }
    }

    // TODO: Fill in the newMeans array
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(
        s"""Iteration: $iter
           |  * current distance: $distance
           |  * desired distance: $kmeansEta
           |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
        println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
          f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while (idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      val langLabel: String = // most common language in the cluster
        langs(calculateCommon(vs) / langSpread)
      val langPercent: Double = // percent of the questions in the most common language
        vs.map(record => {
          if (langLabel.equals(langs(record._1 / langSpread))) {
            1
          } else {
            0
          }
        }).sum * 100.0 / vs.size
      val clusterSize: Int = vs.size

      var medianScore: Int = 0
      val temp = vs.toArray.sortWith((x, y) => x._2.compareTo(y._2) < 0)
      if (vs.size % 2 == 0) {
        medianScore = (temp(vs.size / 2 - 1)._2 + temp(vs.size / 2)._2) / 2
      } else {
        medianScore = temp(vs.size / 2)._2
      }
      // val medianScore: Int = (vs.size / 2)._2

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def calculateCommon(ps: Iterable[(Int, Int)]): Int = {

    import collection.mutable.Map
    var langMap: Map[Int, Int] = Map()
    val iter = ps.iterator
    while (iter.hasNext) {
      val item = iter.next
      if (langMap.contains(item._1)) {
        langMap(item._1) = langMap(item._1) + 1
      } else {
        langMap(item._1) = 1
      }
    }

    var max = -1
    var maxIndex = 0
    for (record <- langMap) {
      if (max < record._2) {
        max = record._2
        maxIndex = record._1
      }
    }
    maxIndex
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}

/*
output:
Iteration: 45
  * current distance: 5.0
  * desired distance: 20.0
  * means:
             (450000,1) ==>           (450000,1)    distance:        0
            (450000,10) ==>          (450000,10)    distance:        0
           (450000,105) ==>         (450000,105)    distance:        0
                  (0,2) ==>                (0,2)    distance:        0
               (0,2309) ==>             (0,2309)    distance:        0
                (0,467) ==>              (0,467)    distance:        0
             (600000,7) ==>           (600000,7)    distance:        0
             (600000,1) ==>           (600000,1)    distance:        0
             (600000,0) ==>           (600000,0)    distance:        0
          (150000,1629) ==>        (150000,1629)    distance:        0
             (150000,3) ==>           (150000,3)    distance:        0
           (150000,292) ==>         (150000,292)    distance:        0
           (300000,105) ==>         (300000,105)    distance:        0
             (300000,3) ==>           (300000,3)    distance:        0
           (300000,557) ==>         (300000,557)    distance:        0
          (50000,10271) ==>        (50000,10271)    distance:        0
            (50000,334) ==>          (50000,334)    distance:        0
              (50000,2) ==>            (50000,2)    distance:        0
             (200000,2) ==>           (200000,2)    distance:        0
            (200000,99) ==>          (200000,99)    distance:        0
           (200000,530) ==>         (200000,530)    distance:        0
           (500000,176) ==>         (500000,176)    distance:        0
            (500000,31) ==>          (500000,31)    distance:        0
             (500000,3) ==>           (500000,3)    distance:        0
           (350000,940) ==>         (350000,940)    distance:        0
             (350000,2) ==>           (350000,2)    distance:        0
           (350000,210) ==>         (350000,210)    distance:        0
             (650000,2) ==>           (650000,2)    distance:        0
            (650000,74) ==>          (650000,74)    distance:        0
            (650000,15) ==>          (650000,15)    distance:        0
          (100000,1263) ==>        (100000,1263)    distance:        0
           (100000,181) ==>         (100000,182)    distance:        1
             (100000,2) ==>           (100000,2)    distance:        0
             (400000,2) ==>           (400000,2)    distance:        0
           (400000,121) ==>         (400000,121)    distance:        0
           (400000,584) ==>         (400000,584)    distance:        0
             (550000,5) ==>           (550000,5)    distance:        0
          (550000,1130) ==>        (550000,1130)    distance:        0
            (550000,66) ==>          (550000,66)    distance:        0
          (250000,1766) ==>        (250000,1766)    distance:        0
           (250000,269) ==>         (250000,271)    distance:        4
             (250000,3) ==>           (250000,3)    distance:        0
             (700000,4) ==>           (700000,4)    distance:        0
             (700000,0) ==>           (700000,0)    distance:        0
            (700000,49) ==>          (700000,49)    distance:        0



Resulting clusters:
  Score  Dominant language (%percent)  Questions
================================================
      0  Groovy            (100.0%)         1631
      0  MATLAB            (100.0%)         3725
      1  C#                (100.0%)       361835
      1  Ruby              (100.0%)        54727
      1  CSS               (100.0%)       113598
      1  PHP               (100.0%)       315771
      1  Objective-C       (100.0%)        94745
      1  JavaScript        (100.0%)       365649
      1  Java              (100.0%)       383473
      2  Perl              (100.0%)        19229
      2  MATLAB            (100.0%)         7989
      2  Clojure           (100.0%)         3441
      2  Python            (100.0%)       174586
      2  C++               (100.0%)       181255
      2  Scala             (100.0%)        12423
      3  Groovy            (100.0%)         1390
      4  Haskell           (100.0%)        10362
      5  MATLAB            (100.0%)         2774
      9  Perl              (100.0%)         4716
     14  Clojure           (100.0%)          595
     25  Scala             (100.0%)          728
     38  Groovy            (100.0%)           32
     53  Haskell           (100.0%)          202
     66  Clojure           (100.0%)           57
     79  C#                (100.0%)         2585
     79  Perl              (100.0%)           56
     85  Ruby              (100.0%)          648
     97  Objective-C       (100.0%)          784
    130  Scala             (100.0%)           47
    139  PHP               (100.0%)          475
    173  CSS               (100.0%)          358
    213  C++               (100.0%)          264
    227  Python            (100.0%)          400
    249  Java              (100.0%)          483
    377  JavaScript        (100.0%)          431
    443  C#                (100.0%)          147
    503  Objective-C       (100.0%)           73
    549  Ruby              (100.0%)           34
    769  CSS               (100.0%)           26
    887  PHP               (100.0%)           13
   1269  Python            (100.0%)           19
   1290  C++               (100.0%)            9
   1602  Haskell           (100.0%)            2
   1895  JavaScript        (100.0%)           33
  14639  Java              (100.0%)            2


 */