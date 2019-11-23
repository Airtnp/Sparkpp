// spark-shell --executor-memory=6g

import scala.io.Source

val files = 0 until 40

spark.time(sc
      .parallelize(files, 40)
      .flatMap(idx => Source.fromFile(f"/home/ubuntu/Sparkpp/examples/input/input_$idx")
                              .getLines)
                              .flatMap(l => l.split(' ')))
      .map(s => (s, 1))
      .reduceByKey(_ + _, 8)
      .collect())