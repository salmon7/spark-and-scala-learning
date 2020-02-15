# 维基百科词条最热门编程语言

通过几种计算方式计算维基百科词条中出现某些编程语言的文档量，并按 __文档数量的大小__ 为标准评判热门程度。


## 数据来源
> 数据来源于coursera

文本集在 `src/main/resources/wikipedia/wikipedia.dat` 中，每行代表一个html文档，包括<titile>和<text>两个主要部分，titile表示标题，text表示正文文本。此处做简化处理，只要正文文本text出现了该语言，则认为该文档提及到了该编程语言。

## 计算方式

实现了几种计算方式，并比较其计算时间分析其差异的原因。假设有n个编程语言，m个文档。

- 第一种：用每个langs遍历rdd数据集，过滤并统计其个数，最后再返回给driver。
	- 计算时间复杂度为 n*m + n个job
- 第二种：计算倒排索引 __RDD[(String, Iterable[WikipediaArticle])]__，每个langs都对应一个可遍历的WikipediaArticle，最后使用mapValues把Value类型从Iterable[WikipediaArticle]转为Int。
	- 计算时间复杂度为 n*m + 1个job + shuffle 所有文档
- 第三种：计算langs和WikipediaArticle 的笛卡尔积，filter过滤，map转换value类型，最后进行reduceByKey，进行本地的merge和不同partition的combine。
	- 时间复杂度 n*m + 1个job + shuffle 部分Int

## 结果

最后的输出结果为：

```
Processing Part 1: naive ranking took 61882 ms.
Processing Part 2: ranking using inverted index took 27690 ms.
Processing Part 3: ranking using reduceByKey took 20043 ms.
```

明显可以看出第一种计算方式最慢，其主要原因还是一个job只能完成一个langs的计算，有n个langs就需要进行n次job，而spark的调度时间明显延长了整体运行时间。  

第二种计算方式只需要启动一个job，遍历次数也为n*m。由于其只需要一个job，所以第一种计算方式快了很多。在建立倒排索引的时候，groupByKey在不同parittion进行了shuffle，除了paritioner计算前后依然是同一个partition的文档，其他文档均需要跨partition传输，  

第三种计算方式也是只需要启动一个job，遍历次数也为n*m，job个数也为1。map和reduceByKey的引入带来了两个优点，一是可以传一个int类型而不是整个文档，二是本地先merge，减少跨parition的数据传输。






