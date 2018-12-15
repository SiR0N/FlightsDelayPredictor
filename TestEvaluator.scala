import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

object TestEvaluator {


def evalue:(DataFrame, String, String ) => Array[Double]= (df: DataFrame, label: String, predic: String) =>{

  val eval_rmse = new RegressionEvaluator()
    .setLabelCol(label)
    .setPredictionCol(predic)
    .setMetricName("rmse")

  val eval_r2 = new RegressionEvaluator()
    .setLabelCol(label)
    .setPredictionCol(predic)
    .setMetricName("r2")

   Array(eval_rmse.evaluate(df), eval_r2.evaluate(df))

}
  def printMetrics(array: Array[Double]): Unit ={

    val mrse_testing = array(0)
    val r2_testing = array(1)
    println("Metrics about the testing set:")
    println("RMSE testing: " + mrse_testing )
    println("r2 testing: " + r2_testing)
  }
}