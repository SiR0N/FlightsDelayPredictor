import UDFusage.{converttoTotalMin, getRushHour, isHoliday}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col



object MakeUpDataSets {
  def MakeUP:(String) => DataFrame = (txt: String) =>{
  val conf = new SparkConf()
    .setAppName("Flights Delay")
    .setMaster("local[*]")
  new SparkContext(conf)
  /* val cont = new SparkContext(conf)
   cont.setLogLevel("WARN")*/


  val spark = SparkSession.builder().getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._


  println("Welcome to " + spark.sparkContext.appName + " Calculator")

  println("LOADING the data from CSV files....")
  val flights = spark.read.option("header", "True")
    .format("com.databricks.spark.csv")
    //.load(args(0))
    .csv(txt)


  println("SCHEMA of the data we are going to work with:")
  flights.printSchema
  flights.show(2)
  println("Filling our Data Frame...")
  println("Deleting variables which we are not going to use")


  val df1 = flights.drop("ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn",
    "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")

  println("let´s see the new SCHEMA:")
  df1.printSchema
  println("our 20 first elements:")
  df1.show(20)
  println("Remove cancelled flight:")
  val df2 = df1.filter($"Cancelled" === 0)    // Filter out all cancelled flights, they are useless for us (1 = cancelled)

  /* val df2 = df1.select(col("*"))
     .where(col("Cancelled") === "0")*/
  df2.show(12)

  df2.select("*").where(col("CancellationCode") =!= "null").show()
  println("I found a wrong row!!")
  println("Let´s delete Canceled and CancellationCode")
  val df3 = df2.drop("Cancelled", "CancellationCode")

  val df4 = df3.na.drop() //we can avoid it, it doesnt make any change

  val df5 = df4
    .filter($"ArrDelay".isNotNull)
    .withColumn("Year", col("Year").cast("integer"))
    .withColumn("DepTime", converttoTotalMin(col("DepTime")))
    .withColumn("CRSDepTime", converttoTotalMin(col("CRSDepTime")))
    .withColumn("RushHour", getRushHour(col("CRSDepTime"))) // Create a new var RushHour
    .withColumn("CRSArrTime", converttoTotalMin(col("CRSArrTime")))
    .withColumn("CRSElapsedTime", col("CRSElapsedTime").cast("integer"))
    .withColumn("TypeDAY", isHoliday(col("Month"), col("DayofMonth")))
    .withColumn("ArrDelay", col("ArrDelay").cast("integer"))
    .withColumn("DepDelay", col("DepDelay").cast("integer"))
    .withColumn("Distance", col("Distance").cast("integer"))
    .withColumn("TaxiOut", col("TaxiOut").cast("integer"))


  df5.printSchema()
  df5.show(15)

    val dfF = df5
    dfF.cache()
    //START MLIB
    println("The preprocessing of the data is finished, let´s show how it looks:")
    dfF.printSchema()
    dfF.show(12)
    //println("Number of ROWs:"+dfF.count()) 7M
    println("Now it is time to start to aplly ML...")

    dfF

    //////////////////SOME PREVIOUS TESTING
  /*var week = new Array[Int](7)
  var month = new Array[Int](12)
  var dMonth = new Array[Int](31)
  println("Looking for number of flights in months and days ")
  for( n <- 1 to 31){
    if (n < 8){
      week(n-1) = df5.select("DayOfWeek").where(col("DayOfWeek") === n).count().toInt

      println("Day "+ n+ ", Flights: "+ week(n-1))
            }
   if(n < 13){ month(n-1) = df5.select("Month").where(col("Month") === n).count().toInt
    println("Month "+ n+ ", Flights: "+ month(n-1))
   }
    dMonth(n-1) = df5.select("DayofMonth").where(col("DayofMonth") === n).count().toInt

    println("DayofMonth "+ n+ ", Flights: "+ dMonth(n-1))
  }*/
  // df5.groupBy("DayOfWeek").mean("ArrDelay").show()
  // df5.groupBy("Month").mean("ArrDelay").show()
  //df5.groupBy("DayofMonth").mean("ArrDelay").show(31)
  //df5.groupBy("Origin").mean("ArrDelay").show(30)
  //df5.groupBy("Dest").mean("ArrDelay").show(30)
  /*println("tailmun"+df5.groupBy("TailNum").mean("ArrDelay").count())
  println("FlightNum"+df5.groupBy("FlightNum").mean("ArrDelay").count())
    println("UniqueCarrier"+df5.groupBy("UniqueCarrier").mean("ArrDelay").count())*/



  /*
  tailmun5503
FlightNum7593
UniqueCarrier20
  Day 1, Flights: 1090187
  Day 2, Flights: 1052983
  Day 3, Flights: 1062432
  Day 4, Flights: 1072592
  Day 5, Flights: 1077472
  Day 6, Flights: 917348
  Day 7, Flights: 1019453
Month 1, Flights: 605782
Month 2, Flights: 540139
Month 3, Flights: 622332
Month 4, Flights: 603510
Month 5, Flights: 624768
Month 6, Flights: 612037
Month 7, Flights: 635054
Month 8, Flights: 640984
Month 9, Flights: 593680
Month 10, Flights: 622665
Month 11, Flights: 598870
Month 12, Flights: 592646
DayofMonth 1, Flights: 236148
DayofMonth 2, Flights: 239572
DayofMonth 3, Flights: 235558
DayofMonth 4, Flights: 239697
DayofMonth 5, Flights: 243028
DayofMonth 6, Flights: 239935
DayofMonth 7, Flights: 242340
DayofMonth 8, Flights: 240218
DayofMonth 9, Flights: 243782
DayofMonth 10, Flights: 237121
DayofMonth 11, Flights: 240701
DayofMonth 12, Flights: 245017
DayofMonth 13, Flights: 235479
DayofMonth 14, Flights: 237545
DayofMonth 15, Flights: 238316
DayofMonth 16, Flights: 238807
DayofMonth 17, Flights: 236748
DayofMonth 18, Flights: 242962
DayofMonth 19, Flights: 244682
DayofMonth 20, Flights: 242570
DayofMonth 21, Flights: 241176
DayofMonth 22, Flights: 235871
DayofMonth 23, Flights: 240224
DayofMonth 24, Flights: 233880
DayofMonth 25, Flights: 237882
DayofMonth 26, Flights: 244306
DayofMonth 27, Flights: 237556
DayofMonth 28, Flights: 239035
DayofMonth 29, Flights: 220549
DayofMonth 30, Flights: 222883
DayofMonth 31, Flights: 138879



*/

  /*
  +---------+------------------+
|DayOfWeek|     avg(ArrDelay)|
+---------+------------------+
|        1|10.513502556550229|
|        6| 5.846600031017031|
|        3| 9.962943847767281|
|        5|13.067675000697863|
|        4|12.685980155261941|
|        7| 10.32957740663109|
|        2| 8.263684434009868|
+---------+------------------+

+-----+------------------+
|Month|     avg(ArrDelay)|
+-----+------------------+
|   12|16.213714049846818|
|    1| 9.162151701506165|
|    6| 16.17952800579826|
|    3|10.084908470559062|
|    5| 7.037888681043307|
|    9|3.7494980749698845|
|    4| 8.516229825822615|
|    8| 12.57153344196042|
|    7|14.107679837700504|
|   10|6.5082592714725775|
|   11| 4.793344024722863|
|    2| 13.51979483296776|

+----------+------------------+
|DayofMonth|     avg(ArrDelay)|
+----------+------------------+
|        31| 8.723496684153936|
|        28| 9.576280422881192|
|        26| 13.72403531599276|
|        27|11.541280220870394|
|        12| 9.072278511561699|
|        22|11.339179895416063|
|         1|11.008046572457369|
|        13| 7.617002972666797|
|         6| 7.944046953138697|
|        16|13.845152427881889|
|         3| 8.100349452411052|
|        20| 9.110151856714642|
|         5|10.067957882933023|
|        19|13.894808877524259|
|        15|15.093342308420455|
|         9| 9.654639175257731|
|        17|11.130094276122783|
|         4| 7.413523399452376|
|         8|  7.78527891462972|
|        23| 9.410991083461637|
|         7|  7.94767843192368|
|        10| 9.396325459317586|
|        25|11.836422080575648|
|        24| 8.182673674823796|
|        29| 9.363664887183171|
|        21|11.613535973646776|
|        11|  9.89038648167971|
|        14|11.014712958038947|
|         2| 9.192262014284998|
|        30| 9.410920509809744|
|        18|11.221029184283799|
+----------+------------------+
  Origin|      avg(ArrDelay)|
+------+-------------------+
|   BGM|  10.70414847161572|
|   DLG| 16.771929824561404|
|   PSE|-0.7139959432048681|
|   INL| 13.556122448979592|
|   MSY|  7.484378390106314|
|   GEG|  4.217902237779723|
|   SNA|  5.738060837234391|
|   BUR|  6.311665735394549|
|   GRB| 11.140291384074354|
|   GTF| 3.2035087719298247|
|   IDA|  3.158759124087591|
|   GRR|  9.916221145984023|
|   LWB|               14.6|
|   EUG|   6.91008174386921|
|   PSG| 11.888726207906295|
|   PVD|  8.517959944863374|
|   GSO| 11.738418669453152|
|   MYR| 14.182771194165907|
|   ISO|                3.0|
|   OAK|  5.678574812849427|
|   MSN| 12.911526479750778|
|   COD|-0.9716475095785441|
|   BTM| 1.4120603015075377|
|   FAR| 11.733463414634146|
|   FSM| 10.098542944785276|
|   MQT|  9.395874263261296|
|   SCC|  9.048991354466859|
|   DCA|  10.43320336072042|
|   RFD| 2.9825581395348837|
|   MLU| 13.448170731707316|
+------+-------------------+
only showing top 30 rows

+----+------------------+
|Dest|     avg(ArrDelay)|
+----+------------------+
| BGM| 8.138979370249729|
| PSE| 4.808274470232089|
| DLG|7.9298245614035086|
| INL|  8.83248730964467|
| MSY| 8.137504399382733|
| GEG| 6.982784431137724|
| SNA|  5.35364704524913|
| BUR| 7.214500860585198|
| GRB|13.292747087561068|
| GTF| 8.002102312543798|
| IDA| 4.399276236429433|
| GRR|13.141510619580243|
| LWB|10.336898395721924|
| PVU|              null|
| EUG| 8.186402808809447|
| PSG|13.037845705967976|
| PVD|11.166707090306412|
| GSO|12.631608120279914|
| MYR|17.400785261250377|
| ISO| 9.142857142857142|
| OAK|5.2826478027403185|
| MSN|14.674036979969184|
| COD| 4.567588325652841|
| FAR|12.302646944336317|
| BTM|  5.55050505050505|
| FSM| 12.79077743902439|
| MQT|26.519061583577713|
| SCC| 9.222857142857142|
| DCA|  9.79711823573017|
| RFD| 10.24563953488372|
+----+------------------+
only showing top 30 rows

+-------+------------------+
|TailNum|     avg(ArrDelay)|
+-------+------------------+
| N672SW| 9.066176470588236|
| N866AS| 5.065714285714286|
| N466SW| 6.456533055700156|
| N516UA|16.637017070979336|
| N919UA|10.596166134185303|
| N513UA|14.075690115761354|
| N102UW| 13.34153846153846|
| N902DE| 7.256837098692033|
|  N6700|7.9083484573502725|
| N388DA|  5.22615219721329|
| N686AE|16.068336162988114|
| N502US|20.863192182410423|
| N8934E| 13.15893470790378|
| N607NW|16.344424460431654|
| N331NB|  9.57426528991263|
| N369NB| 9.263878875270368|
| 89709E| 8.836143308746049|
| N396AA|17.034749034749034|
| N3CWAA|12.934611048478017|
| N407AA|15.970326409495549|
| N4YUAA|13.678321678321678|
| N499AA| 13.58041958041958|
| N567AA|10.576599326599327|
| N33637|12.113714679531357|
| N73283| 7.779482262703739|
| N27015|              -4.8|
| N622SW|  5.93236173393124|
| N10575|13.950405770964833|
| N13118| 9.756507136859781|
| N517UA| 13.50412087912088|
+-------+------------------+
only showing top 30 rows

+---------+--------------------+
|FlightNum|       avg(ArrDelay)|
+---------+--------------------+
|      675|   7.414285714285715|
|     1090|   4.469688385269122|
|     2294|  10.512304250559284|
|      296|   11.12405176260598|
|     1436|   15.46587406428886|
|     2088|  17.072434607645874|
|     2162|  13.670926517571885|
|     1159|  6.0217681030653045|
|      691|  12.505130228887134|
|      467|   7.063368267121619|
|     2069|  3.3062381852551983|
|     1512|   9.760696517412935|
|     2136|   4.132530120481928|
|     1572|   5.537626357169599|
|     2904|   4.822475570032573|
|     3210|   9.338954468802697|
|     7273|  11.243856332703213|
|     7252|  22.869127516778523|
|     4937|  14.807560137457045|
|     5645|    5.89364161849711|
|     6240|  17.209205020920503|
|     6731|   2.669260700389105|
|     3959|  12.724540901502504|
|     4032|  13.700647249190938|
|      829|    8.41075429424944|
|     4821|  26.951115834218918|
|     3606|   16.34011627906977|
|     5325|-0.32075471698113206|
|     6194|  18.623711340206185|
|     5925|   8.328931572629052|
+---------+--------------------+
only showing top 30 rows

+-------------+-------------------+
|UniqueCarrier|      avg(ArrDelay)|
+-------------+-------------------+
|           UA| 12.753122052442935|
|           AA| 14.441868837930652|
|           NW| 12.559141798399496|
|           EV|  17.19572911281664|
|           B6| 13.439935654887877|
|           DL|  7.303038714733208|
|           OO|   8.83888716371204|
|           F9|  7.431885488647581|
|           YV| 10.963476476104535|
|           US| 11.527268636599711|
|           AQ|-1.3768957938942925|
|           MQ| 12.992793560697784|
|           OH| 13.135050852894137|
|           HA|-0.4305175650308394|
|           XE| 10.007051775375617|
|           AS|  9.262787752270514|
|           FL|  7.937922568678834|
|           CO| 10.269463366944708|
|           WN|  5.472958396069693|
|           9E|  8.138337077214553|
+-------------+-------------------+
   */




  }

}
