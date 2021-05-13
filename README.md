# Isolation Forest anomaly detection for merchant data
## _Suqiang(Jack) Song , Mastercard_
## Background
Given Mastercard worldwide large collection of 85 million active merchant locations and 19 k aggregate merchants ( brands)  , how do we algorithmically identify anomaly bases on key metrics ? Our assumption is to take advantage of the fact that for any aggregate merchant, the relatively locations that are problematic often appear very different and produce visitation patterns that are very different from locations of other aggregate merchants. In other words, we can treat this as an outlier detection problem. This problem then breaks down into:
1.Identifying and creating relevant metrics(features) for each aggregate merchant by computing periodical aggregation from raw transactions with location matched
2.Train Anomaly Detection models can cover multivariate aggregate merchants based on the features using outlier detection methods.
3.Generating an outlier score for each aggregate merchant serving the models within conditional context ( time series , country(region) etc.)
4.Validating outlier detection methods

## Application cases
- Merchant Auditing. Take into account the aggregate merchants which have anomaly data points as high priority list of merchant auditing list.
- KPI Monitoring & Alerting of Aggregate merchant.  Relevant business applications can take advantage of those anomaly detection to trigger warning and alerting to business context.

## Pros and cons of isolation forest 
Pros:
- [Unsupervised algorithm] - Do NOT need labelled data
- [Free Data distribution] - Do Not Require data to be normally distributed
- [Works with multivariate higher dimensional data] - Able to consider data patterns from similar aggregate merchants.

Cons
- [Overfitting] - Overfitting data points in similar clusters due to random samples selection. High false positive rate due to detect anomaly data points compared with unrelate data points
- [Unbalance performance ] - Do not work well with elements with fewer data points

## Features
- We developed our customized Scala-Spark Isolation Forest program which takes into the outlier outputs from Z-Score as features to train the model.
- Serve the trained Isolation Forest model with target periodical aggregate merchant metrics and assign the isolation forest outlier label to each data point which already been tagged with Z-Sore outlier label , then filter output of Z-Score outliers (isolation_forest_outlier_label=1 and Z-Score_outlier_label=1).
- Generate different period metrics to make up the periodical trends shifts.
- Add "withPartition" mode which can pre-partition data sets and generate model , serve model for each partition

## Data Inputs
The training and test datasets are published after data masking processing.
Below are some data samples 
```sh
txn_date,agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_channel,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend,dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier
11/6/2020,1201920,1,1,1,1,1,1,0,1,1,0,1,0,16.23,16.23,5,0,0,0,0
12/4/2020,1201920,1,1,1,1,1,1,0,1,1,0,1,0,47.16,47.16,5,0,0,0,0
11/22/2020,1282048,1,1,1,1,1,1,0,2,2,0,2,0,135,120.495,7,0,0,0,0
```
## Installation
This project requires Spark to run. If you already have Spark runtime environment , then you need to complie the project and build the fat jar , upload the prepared data sets ,then run the execution scripts . We recommend to run the Docker environment below.
### Building for source
```sh
git clone https://github.com/Mastercard/Udap-Merchant-Iso-Forest.git
cd Udap-Merchant-Iso-Forest
mvn clean install package
mv target/merchant-iso-forest-1.0-SNAPSHOT-jar-with-dependencies.jar scripts/merchant-iso-forest.jar
```
### Docker
We make it very easy to install and deploy in a Docker container.When ready, simply use the Dockerfile to
build the image.

```sh
cd Udap-Merchant-Iso-Forest
docker build --rm -t merchant-iso:latest .
```

This will create the merchant-iso image and pull in the necessary dependencies.

Once done, run the Docker image

```sh
docker run -it --rm  \
-p 22:22 -p 4040:4040 -p 6006:6006 -p 8080:8080 \
-e RUNTIME_DRIVER_CORES=1 \
-e RUNTIME_DRIVER_MEMORY=2g \
-e RUNTIME_EXECUTOR_CORES=2 \
-e RUNTIME_EXECUTOR_MEMORY=6g \
-e RUNTIME_TOTAL_EXECUTOR_CORES=2 \
--name merchant-iso -h merchant-iso merchant-iso:latest bash
```
> Note: `mv merchant-iso-forest.jar to scripts folder ` is required before docker build

### Parameters explaination 
This project got insprations from LinkedIn open source project 
https://github.com/linkedin/isolation-forest
So you can understand the same parameter set as below 
|      Parameter     |   Default Value  |                                                                                                                                                                                      Description                                                                                                                                                                                     |   |   |
|:------------------:|:----------------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------:|---|---|
| numEstimators      | 100              | The number of trees in the ensemble.                                                                                                                                                                                                                                                                                                                                                 |   |   |
| maxSamples         | 256              | The number of samples used to train each tree. If this value is between 0.0 and 1.0, then it is treated as a fraction. If it is >1.0, then it is treated as a count.                                                                                                                                                                                                                 |   |   |
| contamination      | 0.0              | The fraction of outliers in the training data set. If this is set to 0.0, it speeds up the training and all predicted labels will be false. The model and outlier scores are otherwise unaffected by this parameter.                                                                                                                                                                 |   |   |
| contaminationError | 0.0              | The error allowed when calculating the threshold required to achieve the specified contamination fraction. The default is 0.0, which forces an exact calculation of the threshold. The exact calculation is slow and can fail for large datasets. If there are issues with the exact calculation, a good choice for this parameter is often 1% of the specified contamination value. |   |   |
| maxFeatures        | 1.0              | The number of features used to train each tree. If this value is between 0.0 and 1.0, then it is treated as a fraction. If it is >1.0, then it is treated as a count.                                                                                                                                                                                                                |   |   |
| bootstrap          | false            | If true, draw sample for each tree with replacement. If false, do not sample with replacement.                                                                                                                                                                                                                                                                                       |   |   |
| randomSeed         | 1                | The seed used for the random number generator.                                                                                                                                                                                                                                                                                                                                       |   |   |
| featuresCol        | "features"       | The feature vector. This column must exist in the input DataFrame for training and scoring.                                                                                                                                                                                                                                                                                          |   |   |
| predictionCol      | "predictedLabel" | The predicted label. This column is appended to the input DataFrame upon scoring.                                                                                                                                                                                                                                                                                                    |   |   |
| scoreCol           | "outlierScore"   | The outlier score. This column is appended to the input DataFrame upon scoring.                                                                                                                                                                                                                                                                                                      |   |   |

To simply the usage , also we added addtional features , we use those parameters 

```sh
    debug:              Boolean         = true,
    withLabel:          Boolean         = true,
    withPartition:      Boolean         = false,
    mode:               String          = Mode_Train,
    categoricalColumns: String          = "col1,col2",
    numericColumns:     String          = "col3,col4",
    partitionColumn:    String          = "col5",
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
```
We recommend to run those scripts for training and inference , you can tune Spark relevant parameters depend on your spark environment setting.
### Spark submit script for training 
```sh
#!/bin/bash
spark-submit \
--master ${RUNTIME_SPARK_MASTER} \
--class com.mastercard.eds.mds.mqa.iso.MerchantIsoForest \
--executor-memory 4G \
--driver-memory 2g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.ui.showConsoleProgress=false \
--conf spark.yarn.max.executor.failures=4 \
--conf spark.executor.memoryOverhead=512 \
--conf spark.driver.memoryOverhead=512 \
--conf spark.sql.tungsten.enabled=true \
--conf spark.locality.wait=1s \
--conf spark.yarn.maxAppAttempts=2 \
--conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
merchant-iso-forest.jar \
--mode "train" \
--withLabel true \
--withPartition false \
--partitionColumn "agg_merch_country_index" \
--dataPathParams "/opt/work/data/curated_merchant_agg_train.csv, /opt/work/data/curated_merchant_agg_test.csv, csv, true" \
--TrainParam "12345, 0.8, true, /opt/work/model/" \
--categoricalColumns "dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier" \
--numericColumns "agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend" \
--fineTuningParams "100,false,256,1.0,0.02,12345"
```
### Spark submit script for inference 
```sh
#!/bin/bash
spark-submit \
--master ${RUNTIME_SPARK_MASTER} \
--class com.mastercard.eds.mds.mqa.iso.MerchantIsoForest \
--executor-memory 4G \
--driver-memory 1g \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.ui.showConsoleProgress=false \
--conf spark.yarn.max.executor.failures=4 \
--conf spark.executor.memoryOverhead=512 \
--conf spark.driver.memoryOverhead=512 \
--conf spark.sql.tungsten.enabled=true \
--conf spark.locality.wait=1s \
--conf spark.yarn.maxAppAttempts=2 \
--conf spark.serializer=org.apache.spark.serializer.JavaSerializer \
merchant-iso-forest.jar \
--mode "inference" \
--withLabel true \
--withPartition false \
--partitionColumn "aggregate_merchant_index" \
--dataPathParams "/opt/work/data/curated_merchant_agg_train.csv,/opt/work/data/curated_merchant_agg_test.csv, csv, true" \
--InferenceParam "/opt/work/model/,/opt/work/output/" \
--categoricalColumns "dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier" \
--numericColumns "agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend" \
--fineTuningParams "100,false,256,1.0,0.02,12345"
```
## How to run 
### Run training 
First , execute run_train_iso.sh to generate model 
```sh
cd Udap-Merchant-Iso-Forest
./run_train_iso.sh
...
AppParams are AppParams(true,true,false,train,dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier,agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend,agg_merch_country_index,DataPathParam(/opt/work/data/curated_merchant_agg_train.csv,/opt/work/data/curated_merchant_agg_test.csv,csv,true),TrainParam(12345,0.8,true,/opt/work/model/),InferenceParam(/opt/work/model/,/opt/work/output/),FineTuningParam(100,false,256,1.0,0.02,12345))
...
trainingCount is  409769 parameter maxSamples  is 256
21/05/13 05:36:50 INFO IsolationForest: User specified number of features used to train each tree over total number of features: 25 / 25
21/05/13 05:36:51 INFO IsolationForest: User specified number of samples used to train each tree over total number of samples: 256 / 409769
21/05/13 05:36:51 INFO IsolationForest: Expectation value for samples drawn for each tree is 368.0 samples. This subsample is later limited to user specified 256 samples before training.
21/05/13 05:36:51 INFO IsolationForest: The subsample for each partition is sampled from input data using sampleFraction = 8.980669596772816E-4.
21/05/13 05:36:51 INFO IsolationForest: Training 100 isolation trees using 100 partitions.
...
21/05/13 05:37:08 INFO IsolationForestModelReadWrite$IsolationForestModelWriter: Saving IsolationForestModel metadata to path /opt/work/model/metadata
21/05/13 05:37:08 INFO IsolationForestModelReadWrite$IsolationForestModelWriter: Saving IsolationForestModel tree data to path /opt/work/model/data
test auroc  = 0.934517511698956
training finished!
```
### Run inference 
Then execute run_inference_iso.sh to serve model and get predicted outlier score
```sh
cd Udap-Merchant-Iso-Forest
./run_inference_iso.sh
...
AppParams are AppParams(true,true,false,inference,dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier,agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend,aggregate_merchant_index,DataPathParam(/opt/work/data/curated_merchant_agg_train.csv,/opt/work/data/curated_merchant_agg_test.csv,csv,true),TrainParam(12345,0.9,true,/opt/work/model/),InferenceParam(/opt/work/model/,/opt/work/output/),FineTuningParam(100,false,256,1.0,0.02,12345))
...
21/05/13 05:39:29 INFO IsolationForestModelReadWrite: Loading IsolationForestModel metadata from path /opt/work/model/metadata
21/05/13 05:39:29 INFO IsolationForestModelReadWrite: Loading IsolationForestModel tree data from path /opt/work/model/data
...
inference auroc  = 0.9005795694379782
+---------+-----------------------+----------+----------+---------------+--------+---------+----------+---------+--------------+---------+-----------+--------+----------+------------+----------+---------+----------------------------+-----------------------+-----------------+-----+-------------------+--------------+
| txn_date|agg_merch_country_index|n_location|n_acct_nbr|n_dw_product_cd|n_issuer|n_channel|n_postcode|n_paypass|n_card_present|n_xborder|n_recurring|n_credit|n_business|median_spend|mean_spend|dayofweek|flag_location_visits_outlier|flag_paid_accts_outlier|flag_txns_outlier|label|       outlierScore|predictedLabel|
+---------+-----------------------+----------+----------+---------------+--------+---------+----------+---------+--------------+---------+-----------+--------+----------+------------+----------+---------+----------------------------+-----------------------+-----------------+-----+-------------------+--------------+
| 4/5/2021|                1742784|         1|         1|              1|       1|        1|         1|        0|             1|        1|          0|       1|         0|      141.84|    141.84|        1|                           0|                      0|                0|    0|0.34178774876171225|           0.0|
|3/22/2021|                2033248|         1|         2|              2|       2|        1|         1|        0|             2|        2|          0|       1|         0|       48.92|     48.42|        1|                           0|                      0|                0|    0|  0.341149967382491|           0.0|

```
## Evaluation of Outlier Detection – ROC & AUC
Standard measures for evaluating anomaly detection problems:
--Recall (Detection rate) - ratio between the number of correctly detected anomalies and the total number of anomalies
--False alarm (false positive) rate – ratio between the number of data records from normal class that are misclassified as anomalies and the total number of data records from normal class 
--ROC Curve is a trade-off between detection rate and false alarm rate
--Area under the ROC curve (AUC) is computed using a trapezoid rule
Based on the preapred training and test data sets 
training(test) auroc  = 0.934517511698956
inference auroc  = 0.9005795694379782

## License
Apache 2.0
## Copyright
Copyright (c) 2021 Mastercard


