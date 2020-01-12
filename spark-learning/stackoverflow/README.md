# stackoverflow kmeans聚类分析

根据答案得分将StackOverflow上的帖子针对不同的编程语言进行kmeans聚类。

## 数据来源
> 数据来源于coursera

文本集在 `src/main/resources/stackoverflow/stackoverflow.csv` 中，包含有关StackOverflow帖子的信息，共有8143801个问题或者回答。提供的文本文件中的每一行具有以下格式：

```
<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
```

以下是逗号分隔字段的简短说明。

```
<postTypeId>:     Type of the post. Type 1 = question, 
                  type 2 = answer.
                  
<id>:             Unique id of the post (regardless of type).

<acceptedAnswer>: Id of the accepted answer post. This
                  information is optional, so maybe be missing 
                  indicated by an empty string.
                  
<parentId>:       For an answer: id of the corresponding 
                  question. For a question:missing, indicated
                  by an empty string.
                  
<score>:          The StackOverflow score (based on user 
                  votes).
                  
<tag>:            The tag indicates the programming language 
                  that the post is about, in case it's a 
                  question, or missing in 
```

## 特征向量的计算

1.对每个question，根据id找出其对应的answer，key为question的id，value为 question-answer 元组的集合，命名为grouped。

```scala
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
```

2.在grouped中，对每个question找出所有answer中的最大分值，结果命名为scored，其key为Question，value为HighScore。所以我们的特征向量针对的的是question以及其所有answer中最大的一个。

```scala
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
```

3.继续构造特征向量。对编程语言进行编号，并且扩大不同语言的差异，尽量使kmeans的聚类出来的每个簇中只包含一种语言，所以对编程语言的乘以一个系数langSpread（默认为50000），即跨语言对数据进行分区，然后根据分数进行聚类。在此特征向量vectors构造完成，由于kmeans需要多次迭代vectors，所以对其进行cache。

```scala
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
```

## kmeans聚类

1.在kmeans迭代中，最重要的是两点，一是计算各个特征向量vector距离means的距离并归为其最近中心的类，二是重新计算簇中心。

- 其中findClosest计算一个特征向量计算所有means的距离并找出其归属的类。
- 在for循环中针对所有的簇中心，计算第idx个簇的中心值为该簇所有向量的平均值作为新的簇中心；如果没有属于该簇的特征向量，则保持原来的簇中心不变。
- 根据新旧中心的欧氏距离进行判断是否收敛。如果没有收敛，则再次进行迭代。如果收敛，则聚类结束。

```scala
  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // val newMeans = means.clone() // you need to compute newMeans
    // 新簇中心点
    val newMeans = new Array[(Int, Int)](means.length)

    // 找到所有向量所属的簇，RDD[(Int,(Int,Int))]，key为第n个簇，value表示特征向量
    val closest = vectors.map(p => (findClosest(p, means), p))

    // 求新簇中每个簇中心的中心点
    for (idx <- 0 until kmeansKernels) {
      val vectorArray = closest.filter(_._1 == idx).map(_._2).collect()
      if (vectorArray.length != 0) {
        newMeans(idx) = averageVectors(vectorArray)
      } else {
        newMeans(idx) = means(idx)
      }
    }

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
```

以下是收敛时最后一次迭代的日志输出：

```
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

```

2.最后，还希望输出各个簇的情况，包括

- 最高答案分数的中位数
- 簇中占主导地位的编程语言
- 属于主要语言的答案的百分比
- 簇的大小（以问题维度进行统计）

```
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
```

我们可以简单分析以上的聚类结果：

- 每个簇都只有一种编程语言
- 无论是任何语言，高分答案都偏少，大部分答案都是0~2分

## 初始点的选择

kmeans中簇的个数以及初始点的选择都会影响聚类的结果，本次的选择是针对每个编程语言选3个初始簇的中心，抽样方法选用的是水塘抽样（Reservoir sampling）。下面分析一下水塘抽样算法。

假设数据原始集合很大为n，取k个样本，遍历集合n时的下标即为i：

- 当i<=k时，前i个元素留下的概率为1
- 当i=k+1时，取一个0~1的随机数，比较随机数和 k/(k+1) 的大小，如果随机数小于该值，则第i个需要随机取代原有的样本集中的一个。所以第i个元素留下的概率为 k/(k+1)。对于前k个元素j，其留下来的概率为上一轮中留下的概率乘以这一轮不被取代的概率，即 (k/k) * (1 - k/(k+1) * (1/k)) = k/(k+1)。
- 当i=k+2时，取一个0~1的随机数，比较随机数和 k/(k+2) 的大小，如果随机数小于该值，则第i个需要随机取代原有的样本集中的一个。所以第i个元素留下的概率为 k/(k+2)。对于前k+1个元素j，其留下来的概率为上一轮中留下的概率乘以这一轮不被取代的概率，即 (k/(k+1)) * (1 - k/(k+2) * (1/k)) = k/(k+2)。
- 假设i=m时，每个元素留下的概率均为 k/m。
- 那么当i=m+1时，第i个元素的留下来的概率为 k/(m+1)，前i个元素留下的概率均为：k/m * (1 - k/(m+1) * 1/k) = k/(m+1)，上一轮留下的概率乘以不被替换的概率。
- 综上可知，算法成立。以此类推，直到i遍历完了数据集，即i=n，则每个元素留下的概率均为 k/n 。

## 参考

[水塘抽样(Reservoir Sampling)问题](https://www.cnblogs.com/strugglion/p/6424874.html)  
[Reservoir sampling](https://en.wikipedia.org/wiki/Reservoir_sampling)
