

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import java.util.Calendar


import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics


object MakeYourModel {
  def main(args: Array[String]): Unit = {


    var dfF = MakeUpDataSets.MakeUP(args(0))

    var catColumns = Array("Year", "Dest", "Origin","RushHour", "TypeDAY")
    //there are problems if cat variables have only one values in the ONE

    def filterCat(colu: String): Boolean = dfF.select(colu).distinct().count()>1


    val filtCatColumns = catColumns.filter(filterCat)


    var numericCols = Array("DepTime", "CRSDepTime", "CRSArrTime", "CRSElapsedTime",
                      "DepDelay", "Distance", "TaxiOut")

    val split = dfF.randomSplit(Array(0.8, 0.2))
    val training = split(0).na.drop()
    val testing = split(1).na.drop()

    val training1 = training
    val testing1 = testing

    val stringindexer_stages: Array[org.apache.spark.ml.PipelineStage] = filtCatColumns.map(
      (colName: String) => new StringIndexer()
        .setInputCol(colName)
        .setOutputCol(s"${colName}_indexed")
        .setHandleInvalid("skip") //I added it because I got an error:
      // Caused by: org.apache.spark.SparkException: Unseen label: 9009.  To handle unseen labels, set Param handleInvalid to keep.
      //I choose skip in order to delete the row

    )
    val oneHot_stages: Array[org.apache.spark.ml.PipelineStage] = filtCatColumns.map(
      (colName: String) => new OneHotEncoder()
        .setInputCol(s"${colName}_indexed")
        .setOutputCol(s"${colName}_shoted")
    )

   /* println("Firt of all it is necessary to know the correlation between the target Column and the others Columns")
    for (item <- numericCols++filtCatColumns) {
      println("Correlacion between ArrDelay and " + item + " is: " + training.stat.corr("ArrDelay", item))
    }*/
    /*
Correlacion between ArrDelay and DepTime is: 0.19461222233429748
Correlacion between ArrDelay and CRSDepTime is: 0.13560029627072623
Correlacion between ArrDelay and CRSArrTime is: 0.13152461774377167
Correlacion between ArrDelay and CRSElapsedTime is: 0.005398229306354052
Correlacion between ArrDelay and DepDelay is: 0.9314275496231581
Correlacion between ArrDelay and Distance is: -0.0017679538783730573
Correlacion between ArrDelay and TaxiOut is: 0.3263479365434077
Correlacion between ArrDelay and Month is: -0.009988471656826349       we finally keep it as categorical
Correlacion between ArrDelay and DayofMonth is: 0.0160189575209706     we finally keep it as categorical
Correlacion between ArrDelay and DayOfWeek is: -0.0019879716910744605  we finally keep it as categorical
     */
    println("lets keep just some of them")
    numericCols = Array("DepTime", "DepDelay", "TaxiOut")
    println(s"We are using these Variables: ")
    println("Categorical: " +filtCatColumns.mkString(","))
    println("Numerical: " +numericCols.mkString(","))
    val myColumns = filtCatColumns.map(x => x + "_shoted") ++ numericCols


    val assembler = new VectorAssembler()
      .setInputCols(myColumns)
      .setOutputCol("features")

    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(75)

    val linealR = new LinearRegression()
      .setFeaturesCol("pcaFeatures")
      .setLabelCol("ArrDelay")
      .setSolver("l-bfgs")
      .setElasticNetParam(0.8) //https://spark.apache.org/docs/2.2.0/ml-classification-regression.html
      .setMaxIter(5) //at first it was 10 but we realized that it didnt made significant improvements since the 4th iteration

    println("The pipeline starts here!")
    val timeS = Calendar.getInstance().getTime
    println("time start: " + timeS)


    val pipe = new Pipeline().setStages(stringindexer_stages ++ oneHot_stages ++ Array(assembler, pca, linealR))

    val model = pipe.fit(training)

    model.write.overwrite().save("myModel")

   // val coef = model.stages.last.asInstanceOf[LinearRegressionModel].coefficients

    //println("They are all of the coefficients in the model: " + coef)

    val testPredictions = model.transform(testing)

    testPredictions.printSchema()
    testPredictions.select("*").show()
    testPredictions.select("ArrDelay", "prediction").show()
    val trainPredictions = model.transform(training)

    println("Metrics about our model: ")

    val trainingSummary = model.stages.last.asInstanceOf[LinearRegressionModel].summary
    println("Label:" + trainingSummary.labelCol)
    println("Features" + trainingSummary.featuresCol)
    println("Predictions " + trainingSummary.predictionCol)
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
   //println(trainingSummary.devianceResiduals.mkString(","))
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

println("---------------------------------------")

    TestEvaluator.printMetrics(TestEvaluator.evalue(testPredictions,"ArrDelay","prediction"))

   /*println("now we are going to try to find the best model by CrossValidation.")

    val linealR2 = new LinearRegression() //we want to guess numeric values not categorical
      .setFeaturesCol("pcaFeatures")
      .setLabelCol("ArrDelay")
      .setSolver("l-bfgs")
      .setElasticNetParam(0.8) //https://spark.apache.org/docs/2.2.0/ml-classification-regression.html
      .setMaxIter(5) //at first it was 10 but we realized that it didnt made significant improvements since the 4th iteration


    val pipe2 = new Pipeline()
            .setStages(stringindexer_stages ++ oneHot_stages ++ Array(assembler,pca, linealR2))

    val evaluator = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      //.setPredictionCol("prediction")
      .setMetricName("rmse")

    val paramGrid = new ParamGridBuilder()
      .addGrid(linealR2.elasticNetParam, Array(0.4, 0.6, 0.8))
      .addGrid(linealR2.regParam, Array(0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipe2)
      .setEvaluator(evaluator) // We want the model with the best R-squared
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val eval_rmse = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val eval_r2 = new RegressionEvaluator()
      .setLabelCol("ArrDelay")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    val cvModel = cv.fit(training1)

    val predictions = cvModel.transform(testing1)
    val test_train = cvModel.transform(training1)


    println("training, RMSE: " + eval_rmse.evaluate(test_train))
    println("training: " + eval_r2.evaluate(test_train))
    println("---------------------------------------")
    TestEvaluator.printMetrics(TestEvaluator.evalue(predictions,"ArrDelay","prediction"))
    val timeF = Calendar.getInstance().getTime
    println("time finish: " + timeF)
    ////////////////////
*/

  }
}