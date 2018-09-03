
object Test {

  def main(args: Array[String]): Unit = {
    val starttime=System.nanoTime : Double  //系统纳米时间
    var j = 0
    for (i <- 1 to 100000){
      j += i
    }
    val endtime=System.nanoTime : Double
    val delta=endtime-starttime
    println(delta/10000000d/1000d)

    val starttime_ = System.currentTimeMillis
    var j_ = 0
    for (i <- 1 to 100000){
      j_ += i
    }
    val endtime_ = System.currentTimeMillis
    println(DateUtil.nowString + " 耗时为： " + (endtime_ - starttime_) + "msec")
  }

}
