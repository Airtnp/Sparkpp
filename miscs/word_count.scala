// spark-shell --executor-memory=6g --master spark://ip-172-31-7-253.us-east-2.compute.internal:7077

import scala.io.Source

val files = 0 until 40

spark.time(sc
      .parallelize(files, 40)
      .flatMap(idx => Source.fromFile(f"/home/ubuntu/Sparkpp/examples/input/input_$idx")
                              .getLines)
                              .flatMap(l => l.split(' ')))
      .map(s => (s, 1))
      .reduceByKey(_ + _, 4)
      .collect())

(sc.parallelize(files, files.length)
  .flatMap(idx => Source.fromFile(f"/home/ubuntu/Sparkpp/examples/input/input_$idx")
                        .getLines
                        .flatMap(l => l.split(' ')))
  .map(s => (s, 1))
  .reduceByKey(_ + _, 4)
  .count())