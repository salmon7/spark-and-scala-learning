package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("wikipedia rank")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(
    for (record <- WikipediaData.lines) yield WikipediaData.parse(record)
  ).cache()

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    //    rdd.filter(_.mentionsLanguage(lang)).count().toInt
    rdd.filter(_.mentionsLanguage(lang))
      .aggregate(0)((pre, now) => {
        if (now.mentionsLanguage(lang)) {
          pre + 1
        } else {
          pre
        }
      }, (pre1, pre2) => pre1 + pre2)
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {

    val result = for (lang <- langs) yield (lang, occurrencesOfLang(lang, rdd))

    result
      .filter(record => {
        record._2 != 0
      })
      .sortWith((record1, record2) => {
        record1._2 > record2._2
      })

  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    val langRdd = sc.parallelize(langs)
    langRdd
      .cartesian(rdd)
      .filter(record => {
        record._2.mentionsLanguage(record._1)
      })
      .groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {

    index
      .mapValues(_.size)
      .collect()
      .sortWith((record1, record2) => {
        record1._2 > record2._2
      })
      .toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val langRdd = sc.parallelize(langs)
    langRdd
      .cartesian(rdd)
      .filter(record => {
        record._2.mentionsLanguage(record._1)
      })
      .map(record => {
        (record._1, 1)
      })
      .reduceByKey(_ + _)
      .collect()
      .sortWith((record1, record2) => {
        record1._2 > record2._2
      })
      .toList
  }

  def main(args: Array[String]): Unit = {

    /* Languages ranked according to (1) */
    // 复杂度: n个job，每个job遍历大小为m的rdd
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    // 复杂度: 1个job，遍历n*m次，有groupByKey的shuffle，并且需要传递lang+原文
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    // 复杂度: 1个job，遍历n*m次，有reduceByKey的shuffle，由于map进行了转换，只需要传递lang+int
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    /*
    Processing Part 1: naive ranking took 61882 ms.
    Processing Part 2: ranking using inverted index took 27690 ms.
    Processing Part 3: ranking using reduceByKey took 20043 ms.
     */
    sc.stop()
  }

  val timing = new StringBuffer

  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
