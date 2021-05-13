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
--partitionColumn "agg_merch_country_index" \
--dataPathParams "/opt/work/data/curated_merchant_agg_train.csv,/opt/work/data/curated_merchant_agg_test.csv, csv, true" \
--InferenceParam "/opt/work/model/,/opt/work/output/" \
--categoricalColumns "dayofweek,flag_location_visits_outlier,flag_paid_accts_outlier,flag_txns_outlier,flag_spend_outlier" \
--numericColumns "agg_merch_country_index,n_location,n_acct_nbr,n_dw_product_cd,n_issuer,n_postcode,n_paypass,n_card_present,n_xborder,n_recurring,n_credit,n_business,median_spend,mean_spend" \
--fineTuningParams "100,false,256,1.0,0.02,12345"
