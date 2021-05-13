package com.mastercard.eds.mds.mqa.iso

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, SQLContext, SaveMode }
import com.linkedin.relevance.isolationforest._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorDisassembler
import org.apache.spark.ml.feature.StandardScaler
import scala.collection.mutable.{ ListBuffer, ArrayBuffer }
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import com.mastercard.eds.mds.mqa.utils.Utils
import com.mastercard.eds.mds.mqa.utils.Utils.AppParams
/**
 * @author suqiang.song
 *
 */
object MerchantIsoForest {

  case class ScoringResult(features: Vector, label: Double, predictedLabel: Double, outlierScore: Double)

  private[mastercard] def getRawDatafromCSV(spark: SparkSession, params: AppParams): DataFrame = {
    val csvPath = if (params.mode == Utils.Mode_Train) params.dataPathParams.dataSourceDirTrain else params.dataPathParams.dataSourceDirInference
    val rawDataDF = spark.read
      .format("csv")
      .option("comment", "#")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    if (params.debug) rawDataDF.show(10)
    rawDataDF

  }

  private[mastercard] def getRawDatafromParquet(spark: SparkSession, params: AppParams): DataFrame = {
    val parquetPath = if (params.mode == Utils.Mode_Train) params.dataPathParams.dataSourceDirTrain else params.dataPathParams.dataSourceDirInference
    val rawDataDF = spark.read.parquet(parquetPath)
    if (params.debug) rawDataDF.show(10)
    rawDataDF
  }

  private[mastercard] def generateFeatures(spark: SparkSession, params: AppParams, rawDataDF: DataFrame): DataFrame = {

    val cols = rawDataDF.columns
    val labelCol = cols.last

    //the columns need OneHo tEncoder
    val categoricalColumns = params.categoricalColumns.split(",")
    /**create a pipeline stage*/
    val stagesArray = new ListBuffer[PipelineStage]()
    for (cate <- categoricalColumns) {
      //use one hot encoder
      val encoder = new OneHotEncoder().setInputCol(cate).setOutputCol(s"${cate}classVec")
      stagesArray.append(encoder)
    }

    val numericCols = params.numericColumns.split(",")
    val assemblerInputs = categoricalColumns.map(_ + "classVec") ++ numericCols
    // VectorAssembler merge all columns to one vector
    val assembler = new VectorAssembler().setInputCols(assemblerInputs).setOutputCol("encoderfeatures")
    stagesArray.append(assembler)
    // normalization
    val scaler = new MinMaxScaler()
      .setInputCol("encoderfeatures")
      .setOutputCol("features")

    stagesArray.append(scaler)

    val pipeline = new Pipeline()
    pipeline.setStages(stagesArray.toArray)
    val pipelineModel = pipeline.fit(rawDataDF)
    var featureDF = pipelineModel.transform(rawDataDF)
    featureDF = if (params.withLabel) featureDF.withColumnRenamed(labelCol, "label") else featureDF.drop(col(labelCol))
    val dropList = categoricalColumns.map(_ + "Index") ++ categoricalColumns.map(_ + "classVec")
    featureDF = featureDF.drop(dropList: _*).drop(col("encoderfeatures")).cache()
    if (params.debug) featureDF.show(10)

    featureDF
  }

  private[mastercard] def getFeatureDF(spark: SparkSession, params: AppParams): DataFrame = {
    params.dataPathParams.dataSourceType match {
      case Utils.Source_CSV =>
        val rawDF = getRawDatafromCSV(spark, params)
        generateFeatures(spark, params, rawDF)
      case Utils.Source_Parquet =>
        val rawDF = getRawDatafromParquet(spark, params)
        generateFeatures(spark, params, rawDF)
    }
  }

  def train(spark: SparkSession, params: AppParams, trainFeatureDF: DataFrame): IsolationForestModel = {
    import spark.implicits._

    val Array(training, test) = trainFeatureDF.randomSplit(Array(params.trainParams.trainSplitRatio, (1 - params.trainParams.trainSplitRatio)), seed = params.trainParams.splitSeed)

    val trainingCount = training.count()
    println("trainingCount is  " + trainingCount + " parameter maxSamples  is " + params.fineTuningParams.maxSamples)
    if (trainingCount < params.fineTuningParams.maxSamples) {
      println("fewer samples are in the input datase which is " + trainingCount + " compared with parameter maxSamples  is " + params.fineTuningParams.maxSamples)
      return null
    }
    //Train the model
    val contamination = params.fineTuningParams.contamination
    val isolationForest = new IsolationForest()
      .setNumEstimators(params.fineTuningParams.numEstimators)
      .setBootstrap(params.fineTuningParams.bootstrap)
      .setMaxSamples(params.fineTuningParams.maxSamples)
      .setMaxFeatures(params.fineTuningParams.maxFeatures)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(contamination)
      .setContaminationError(0.01 * contamination)
      .setRandomSeed(params.fineTuningParams.randomSeed)

    val isolationForestModel = isolationForest.fit(training)
    if (params.trainParams.saveModel) {
      //Persist the trained model on disk

      val modelPathAndName = if (params.withPartition) {
        //get the first record of DataFrame and get the value of partitionColumn
        val firstRec = trainFeatureDF.first()
        val partitionColValue = firstRec.getAs(params.partitionColumn).toString()
        params.trainParams.modelPathAndName + partitionColValue + '/'
      } else params.trainParams.modelPathAndName
      isolationForestModel.write.overwrite.save(modelPathAndName)
    }

    val validationDF = isolationForestModel.transform(test)

    //Score the test data
    // Calculate area under ROC curve and assert
    if (params.withLabel) {
      val scores = validationDF.as[ScoringResult]

      val metrics = new BinaryClassificationMetrics(scores.rdd.map(x => (x.outlierScore, x.label)))

      val auroc = metrics.areaUnderROC()

      println("test auroc  = " + auroc)
    }

    println("training finished! ")
    isolationForestModel
  }

  def inference(spark: SparkSession, params: AppParams, inferenceFeatureDF: DataFrame, model: IsolationForestModel): DataFrame = {
    import spark.implicits._
    //Load the saved model
    val modelPathAndName = if (params.withPartition) {
      //get the first record of DataFrame and get the value of partitionColumn
      val firstRec = inferenceFeatureDF.first()
      val partitionColValue = firstRec.getAs(params.partitionColumn).toString()
      params.inferenceParams.modelPathAndName + partitionColValue + '/'
    } else params.inferenceParams.modelPathAndName
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(modelPathAndName))
    val loadedIsolationForestModel = if (null == model) { if (fileExists) IsolationForestModel.load(modelPathAndName) else null } else model
    if (null == loadedIsolationForestModel) {
      println("The model path was empty, won't do inference " + modelPathAndName)
      return null
    }
    var predictions = loadedIsolationForestModel.transform(inferenceFeatureDF)

    // Calculate area under ROC curve and assert
    if (params.withLabel) {
      val scores = predictions.as[ScoringResult]

      val metrics = new BinaryClassificationMetrics(scores.rdd.map(x => (x.outlierScore, x.label)))

      val auroc = metrics.areaUnderROC()

      println("inference auroc  = " + auroc)
    }

    predictions = predictions.drop(col("features"))

    predictions.show()
    
    if (params.withPartition && !params.mode.equals(Utils.Mode_Pipeline)) predictions.coalesce(1).write.mode("append").parquet(params.inferenceParams.inferenceOutput) else predictions.write.mode("overwrite").parquet(params.inferenceParams.inferenceOutput)

    println("inference finished! ")
    predictions

  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("com.mastercard.eds.mds.mqa.iso").setLevel(Level.INFO)
    val st = System.nanoTime()

    Utils.trainParser.parse(args, Utils.AppParams()).foreach { params =>
      println("AppParams are " + params)
      val spark = SparkSession.builder().appName("MerchantIsoForest").config("spark.driver.allowMultipleContexts", "true").getOrCreate()

      import spark.implicits._

      val mode = params.mode
      mode match {
        case Utils.Mode_Pipeline =>
          // pipeline mode do not support withPartition
          println("Notice : pipeline mode do not support withPartition ! ")
          val trainFeatureDF = getFeatureDF(spark, params)
          val model = train(spark, params, trainFeatureDF)
          //Load and prepare data
          val inferenceFeatureDF = getFeatureDF(spark, params)
          inference(spark, params, inferenceFeatureDF, model)

        case Utils.Mode_Train =>

          val trainFeatureDF = getFeatureDF(spark, params)
          if (params.withPartition) {
            val partitionColName = params.partitionColumn
            val res: Array[DataFrame] = trainFeatureDF.select(partitionColName).distinct().collect().map(_.get(0)).par.map { col1 =>
              trainFeatureDF.filter(trainFeatureDF(partitionColName) === col1).cache()
            }.toArray
            for (subTrainFeatureDF <- res) {
              train(spark, params, subTrainFeatureDF)
            }

          } else {
            train(spark, params, trainFeatureDF)
          }

        case Utils.Mode_Inference =>

          //Load and prepare data
          val inferenceFeatureDF = getFeatureDF(spark, params)
          if (params.withPartition) {
            val partitionColName = params.partitionColumn
            val res: Array[DataFrame] = inferenceFeatureDF.select(partitionColName).distinct().collect().map(_.get(0)).par.map { col1 =>
              inferenceFeatureDF.filter(inferenceFeatureDF(partitionColName) === col1)
            }.toArray
            for (subInferenceFeatureDF <- res) {
              inference(spark, params, subInferenceFeatureDF, null)
            }

          } else {
            inference(spark, params, inferenceFeatureDF, null)
          }

      }

      spark.stop()

    }

  }

}
