package com.mastercard.eds.mds.mqa.utils

import org.apache.spark.SparkContext
import scopt.OptionParser

/**
 * @author suqiang.song
 *
 */
object Utils {
  val Mode_Train = "train"
  val Mode_Inference = "inference"
  val Mode_Pipeline = "pipeline"
  val Source_Parquet = "parquet"
  val Source_CSV = "csv"
  val seperator = ","
  case class AppParams(
    debug:              Boolean         = true,
    withLabel:          Boolean         = true,
    withPartition:      Boolean         = false,
    mode:               String          = Mode_Train,
    categoricalColumns: String          = "col1,col2",
    numericColumns:     String          = "col3,col4",
    partitionColumn:    String          = "col5",
    dataPathParams:     DataPathParam   = DataPathParam("/opt/work/data/shuttle.csv", "/opt/work/data/shuttle.csv", "csv", true),
    trainParams:        TrainParam      = TrainParam(12345, 0.9, true, "/opt/work/model/"),
    inferenceParams:    InferenceParam  = InferenceParam("/opt/work/model/", "/opt/work/output/"),
    fineTuningParams:   FineTuningParam = FineTuningParam(100, false, 256, 1.0, 0.01, 1))

  val trainParser = new OptionParser[AppParams]("Merchant Metrics Anomaly Detection with Isolate Forest ") {
    head("AppParams:")
    opt[String]("dataPathParams")
      .text("dataPath Params")
      .action((x, c) => {
        val pArr = x.split(seperator).map(_.trim)
        val p = DataPathParam(pArr(0), pArr(1), pArr(2), pArr(3).toBoolean)
        c.copy(dataPathParams = p)
      })
    opt[String]("TrainParam")
      .text("Train Params")
      .action((x, c) => {
        val pArr = x.split(seperator).map(_.trim)
        val p = TrainParam(pArr(0).toLong, pArr(1).toDouble, pArr(2).toBoolean, pArr(3))
        c.copy(trainParams = p)
      })
    opt[String]("InferenceParam")
      .text("Inference Params")
      .action((x, c) => {
        val pArr = x.split(seperator).map(_.trim)
        val p = InferenceParam(pArr(0).trim(), pArr(1).trim())
        c.copy(inferenceParams = p)
      })
    opt[String]("fineTuningParams")
      .text("fineTuningParams")
      .action((x, c) => {
        val pArr = x.split(seperator).map(_.trim)
        val p = FineTuningParam(pArr(0).toInt, pArr(1).toBoolean, pArr(2).toLong, pArr(3).toDouble, pArr(4).toDouble, pArr(5).toLong)
        c.copy(fineTuningParams = p)
      })
    opt[String]("mode")
      .text("mode")
      .action((x, c) => c.copy(mode = x))
    opt[String]("partitionColumn")
      .text("partitionColumn")
      .action((x, c) => c.copy(partitionColumn = x))
    opt[String]("categoricalColumns")
      .text("categoricalColumns")
      .action((x, c) => c.copy(categoricalColumns = x))
    opt[String]("numericColumns")
      .text("numericColumns")
      .action((x, c) => c.copy(numericColumns = x))
    opt[Boolean]("debug")
      .text("turn on debug mode or not")
      .action((x, c) => c.copy(debug = x))
    opt[Boolean]("withPartition")
      .text("train and inference with partition or not  ")
      .action((x, c) => c.copy(withPartition = x))
    opt[Boolean]("withLabel")
      .text("train and inference with label or not ")
      .action((x, c) => c.copy(withLabel = x))
  }

  case class DataPathParam(
    dataSourceDirTrain:     String,
    dataSourceDirInference: String,
    dataSourceType:         String,
    saveOverride:           Boolean)

  case class TrainParam(
    splitSeed:        Long,
    trainSplitRatio:  Double,
    saveModel:        Boolean,
    modelPathAndName: String)

  case class InferenceParam(
    modelPathAndName: String,
    inferenceOutput:  String)

  case class FineTuningParam(
    numEstimators: Integer,
    bootstrap:     Boolean,
    maxSamples:    Long,
    maxFeatures:   Double,
    contamination: Double,
    randomSeed:    Long)
}
