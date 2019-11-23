val chunks = 1e6.toLong

val chunkSize = 1e4.toLong

val values = 0L until chunks

def ran(i: Long): Long = {
    var count = 0L
    var prev = i
    for (j <- 0 to 1e4.toInt) {
        prev = (prev * 998244353L + 19260817L) % 134456;
        val x: Double = prev / 67228.0 - 1;
        prev = (prev * 998244353L + 19260817L) % 134456;
        val y: Double = prev / 67228.0 - 1;
        if (x * x + y * y < 1) {
            count += 1;
        }
    }
    count
}

spark.time(sc.parallelize(values, 4)
    .map(i => ran(i))
    .reduce((a, b) => a + b))