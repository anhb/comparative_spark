package com.anhb.comparative.spark

import com.anhb.comparative.spark.process.MainProcess
import com.datio.spark.InitSpark
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

/**
  * Main file for CommunicationResponse process.
  * Implements InitSpark which includes metrics and SparkSession.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * AutoComparative {
  *   ...
  * }
  *
  */
protected trait AutoComparativeTrait extends InitSpark {
  this: InitSpark =>
  /**
    * @param spark  Initialized SparkSession
    * @param config Config retrieved from args
    */
  override def runProcess(spark: SparkSession, config: Config): Int = new MainProcess(spark, config).exitCode()

  override def defineBusinessInfo(config: Config): BusinessInformation =
    BusinessInformation(exitCode = 0, entity = "", path = "", mode = "",
      schema = "", schemaVersion = "", reprocessing = "")

}

object AutoComparative extends AutoComparativeTrait with InitSpark
