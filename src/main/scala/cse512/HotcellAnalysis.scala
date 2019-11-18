package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("PickUpInfo")
  // Get the Trip Count from a certain location on a certain day
  spark.udf.register("isWithinBounds", (x:Long,y:Long,z:Long) => (isWithinBounds(x,y,z)))
  def isWithinBounds(x:Long,y:Long,z:Long): Boolean = {
    return (minX <= x) && (x <= maxX) && (minY <= y) && (y <= maxY) && (minZ <= z && z <= maxZ)
  }

  var filteredPoints = spark.sql("SELECT x, y, z FROM PickUpInfo WHERE isWithinBounds(x,y,z)").persist()
  filteredPoints.createOrReplaceTempView("Filtered_Points")
//  println("Filtered_Points")
//  filteredPoints.show()

  var tripsCountDataFrame = filteredPoints.groupBy("x","y","z").count().persist()
  tripsCountDataFrame.createOrReplaceTempView("Taxi_Trips_Count")
//  println("Taxi_Trips_Count")
//  tripsCountDataFrame.show()
  // Calculate Mean = Number of Pickups/Total Number of Cells
  val mean = filteredPoints.count() / numCells
//  println("mean:",mean)
  val standardDeviationNumerator = spark.sql("SELECT SUM(count*count) FROM Taxi_Trips_Count").first().getLong(0)
//  println("standardDeviationNumerator:",standardDeviationNumerator)
  val standardDeviation = math.sqrt((standardDeviationNumerator / numCells) - (mean * mean))
//  println("standardDeviation:",standardDeviation)

  // Check if the Cell is a Neighbour to the Current Cell of interest
  spark.udf.register("isNeighbour", (currentCellX: Long, currentCellY: Long, currentCellZ: Long, neighbourCellX: Long, neighbourCellY: Long, neighbourCellZ: Long) => (
    isNeighbour(currentCellX.toLong, currentCellY.toLong, currentCellZ.toLong, neighbourCellX, neighbourCellY, neighbourCellZ)
    ))

  def isNeighbour(currentCellX: Long, currentCellY: Long, currentCellZ: Long, neighbourCellX: Long, neighbourCellY: Long, neighbourCellZ: Long): Boolean = {
    //      if(currentCellX == neighbourCellX && currentCellY == neighbourCellY && currentCellZ == neighbourCellZ) {
    //        return false
    //      }
    if ((currentCellX - 1 <= neighbourCellX && neighbourCellX <= currentCellX + 1) &&
      (currentCellY - 1 <= neighbourCellY && neighbourCellY <= currentCellY + 1) &&
      (currentCellZ - 1 <= neighbourCellZ && neighbourCellZ <= currentCellZ + 1)) {
      return true
    } else {
      return false
    }
  }

  // Calculate the number of Neighbours for the current given Cell
  spark.udf.register("getNeighbourCount", (currentCellX: Long, currentCellY: Long, currentCellZ: Long) => (
    getNeighbourCount(currentCellX, currentCellY, currentCellZ)
    ))

  def getNeighbourCount(currentCellX: Long, currentCellY: Long, currentCellZ: Long): Long = {
    var count = 0
    for (x <- (currentCellX - 1) to (currentCellX + 1)) {
      for (y <- (currentCellY - 1) to (currentCellY + 1)) {
        for (z <- (currentCellZ - 1) to (currentCellZ + 1)) {
          if (isWithinBounds(x,y,z)) {
            count += 1
          }
        }
      }
    }
    return count
  }

  // Calculate Getis-Ord Score for current Cell
  spark.udf.register("getGetisOrdScore", (neighbourCount: Long, weightedNeighbourValue: Long) => (
    getGetisOrdScore(neighbourCount, weightedNeighbourValue)
    ))

  def getGetisOrdScore(neighbourCount: Long, weightedNeighbourValue: Long): Double = {
    return (weightedNeighbourValue - mean * neighbourCount) / (standardDeviation * math.sqrt((numCells * neighbourCount - (neighbourCount * neighbourCount)) / (numCells - 1)))
  }

  var neighboursInfo = spark.sql("SELECT c.x,c.y,c.z, getNeighbourCount(c.x,c.y,c.z) as neighbourCount, SUM(n.count) as weightedNeighbourValue " +
    "FROM Taxi_Trips_Count c, Taxi_Trips_Count n " +
    "WHERE isNeighbour(c.x,c.y,c.z,n.x,n.y,n.z) GROUP BY c.x,c.y,c.z").persist()
  neighboursInfo.createOrReplaceTempView("Neighbours_Info")
//  println("Neighbours_Info")
//  neighboursInfo.show()

  // Calculate Getis-Ord Score and sort the view
  var gScoreResult = spark.sql("SELECT x,y,z,getGetisOrdScore(neighbourCount, weightedNeighbourValue) as gScore " +
    "FROM Neighbours_Info ORDER BY gScore DESC")
  gScoreResult.createOrReplaceTempView("GScore_View")
//  println("GScore_View")
//  gScoreResult.show()

  // Select only the required columns
  var result = spark.sql("SELECT x,y,z FROM GScore_View")
//  println("Result")
//  result.show(500)

  return result
}
}
