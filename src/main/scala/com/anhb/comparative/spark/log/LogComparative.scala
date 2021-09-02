package com.anhb.comparative.spark.log

import com.datio.spark.logger.LazyLogging


trait LogComparative extends LazyLogging{

  def printFormatAnalysisResults(formatAnalysisResults: Array[String]): Unit ={formatAnalysisResults.foreach(line => logger.info(line))}

}
