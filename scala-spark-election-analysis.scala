//Here is the exercise - https://hadoopbaseblog.wordpress.com/2017/04/06/exercise-08-scala-and-spark-political-analysis-for-the-state-of-up/

val fileContents = sc.
  textFile("/Users/itversity/Research/data/elections/ls2014.tsv")
val data = fileContents.
  mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
val upData = data.filter(_.split("\t")(0) == "Uttar Pradesh")
val upDataMap = upData.
  map(rec => 
    {
      val r = rec.split("\t")
      ((r(0), r(1)), (r(6), r(8).toInt))
    })

def recalculateWithAlliance(rec: Iterable[(String, Int)]): Iterable[(String, Int)] = {
  rec.map(r => {
    if(r._1 == "INC" || r._1 == "SP")
      ("ALLY", r._2)
    else
      r
  }).
  groupBy(r => r._1).
  map(r => (r._1, r._2.map(_._2).sum))
}


//4 way contest
upDataMap.
  groupByKey().
  map(rec => {
    (rec._1, rec._2.toList.sortBy(s => -s._2))
  }).
  map(rec => (rec._2(0)._1, 1)).
  countByKey().
  foreach(println)

//3 way contest after alliance
upDataMap.
  groupByKey().
  map(rec => (rec._1, recalculateWithAlliance(rec._2))).
  map(rec => {
    (rec._1, rec._2.toList.sortBy(s => -s._2))
  }).
  map(rec => (rec._2(0)._1, 1)).
  countByKey().
foreach(println)
