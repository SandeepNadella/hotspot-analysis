package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var flag = false;
    try {
      if (queryRectangle != null && pointString != null) {
        val pt = pointString.split(",")
        val pointX = pt(0).trim().toDouble
        val pointY = pt(1).trim().toDouble

        val rec = queryRectangle.split(",")
        val recX1 = rec(0).trim().toDouble
        val recY1 = rec(1).trim().toDouble
        val recX2 = rec(2).trim().toDouble
        val recY2 = rec(3).trim().toDouble

        val minRecX = Math.min(recX1, recX2)
        val maxRecX = Math.max(recX1, recX2)
        val minRecY = Math.min(recY1, recY2)
        val maxRecY = Math.max(recY1, recY2)

        if ((pointX >= minRecX) && (pointX <= maxRecX) && (pointY >= minRecY) && (pointY <= maxRecY))
        {
          flag = true
        }
        else
        {
          flag = false
        }
      }
    }
    catch
      {
        case i: NullPointerException => {
          println("Please pass the point and the rectangle")
        }
        case a: ArrayIndexOutOfBoundsException => {
          println("Array Index Out of Bounds, check the inputs.")
        }
        case _: Throwable => println("UnKnown exception")
      }

    return flag
  }
}
