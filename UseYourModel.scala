


import org.apache.spark.ml.{Pipeline, PipelineModel}

object UseYourModel {
    def main(args: Array[String]): Unit = {

    val dfF =  MakeUpDataSets.MakeUP(args(0)).na.drop()
    println("lets start with the pipeline!")
     val model = PipelineModel.load("myModel")


    val testNdF = model.transform(dfF)
      testNdF.show()
      println("Now we need to evaluate our predictions...")


       TestEvaluator.printMetrics(TestEvaluator.evalue(testNdF,"ArrDelay","prediction"))


}
}
