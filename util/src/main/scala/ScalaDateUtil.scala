object ScalaDateUtil {

  def execute(): Unit = {
    val starttime = System.currentTimeMillis
    val endtime = System.currentTimeMillis
    println(DateUtil.nowString + " 耗时为： " + (endtime - starttime) + "msec")
  }

}
