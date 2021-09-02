package com.anhb.comparative.spark.process

import com.anhb.comparative.spark.functions.SparkComparativeFunc._
import com.anhb.comparative.spark.log.LogComparative
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, count, lit, sum}
import org.apache.spark.sql._

class MainProcess(spark: SparkSession, conf: Config) extends LogComparative{

  /*Meter cada dataframe con diferencia en un array de maps utilizando como llave la columna para poder sacar unicamente ese df de acuerdo al nombre de columna*/
  /*Meter IDS en un filtro para poder buscar las diferencias de valores aleatorios*/
  /*Hacer metodo para contar los numeros de registros por llave de un dataframe*/
  /*Hacer un objeto que se agregue los dataframes y el array de columnas para que solo llamen los metodos sin tener que volver a hacer las funciones*/

  def exitCode(): Int = {

    println("First execution")
    //val sample = spark.read.option("header", true).option("delimiter", ",").csv("/home/bleakmurder/Descargas/sample.csv")
    //sample.show()
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/bounces_20201113.txt")//.drop("BounceCategory").drop("BounceCategoryID").drop("SMTPBounceReason").drop("BounceTypeID").drop("BounceType").drop("TriggeredSendCustomerKey").drop("SMTPCode").drop("JobID").drop("BatchID").drop("TriggererSendDefinitionObjectID").drop("BounceSubcategory").drop("BounceSubcategoryID").drop("EventType").drop("EventDate").drop("SMTPMessage")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_BOUNCES.dat")//.drop("BounceCategory").drop("BounceCategoryID").drop("SMTPBounceReason").drop("BounceTypeID").drop("BounceType").drop("TriggeredSendCustomerKey").drop("SMTPCode").drop("JobID").drop("BatchID").drop("TriggererSendDefinitionObjectID").drop("BounceSubcategory").drop("BounceSubcategoryID").drop("EventType").drop("EventDate").drop("SMTPMessage")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/Campaigns_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_CAMPANIAS.dat").drop("CreativeCode").drop("ANIO").drop("MES")
    /*val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/clicks_20201113.txt")//.drop("EventDate").drop("EventType")
    val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_CLICKS.dat")//.drop("EventDate").drop("EventType")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/complaints_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_COMPLAINTS.dat").drop("TriggererSendDefinitionObjectID")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/jobs_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_JOB.dat").drop("CampaignID")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/journey_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_JOURNEY.dat")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/not_sent_20201113.txt").withColumnRenamed("ClientId", "AccountID").withColumnRenamed("TriggeredSendExternalKey", "TriggeredSendCustomerKey")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_NOTSENT.dat").drop("TriggererSendDefinitionObjectID")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/opens_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_OPEN.dat")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/unsubs_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_UNSUBSCRIBES.dat").drop("TriggeredSendCustomerKey").drop("TriggererSendDefinitionObjectID")
    //val df_or = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/or/sent_20201113.txt")
    //val df_stg = spark.read.option("header", true).option("delimiter", "|").csv("/home/antony/usa/develop/samples/SFMC/comparison/new_comparative/stg/GKEXC_D02_20201113_SFMC_FB_SENT.dat")
    val columns_keys = Seq("SubscriberKey", "BatchID", "JobID")//, "IsUnique")//UnsubscribesID//, "IsUnique", "Domain")//OpenMoreID//, "IsUnique", "EmailAddress") //ClicksMoreID//, "LinkContent", "IsUnique") //Bounces, Complaints, Unsubs, Open, Sent, Not_sent, clicks
    //val columns_keys = Seq("SubscriberKey", "BatchID", "JobID", "Domain", "IsUnique", "LinkContent")//Clicks
    //val columns_keys = Seq("SubscriberKey", "BatchID", "JobID", "Domain", "IsUnique") //Open
    //val columns_keys = Seq("CampaignID") //,"CampaignCode") //Campaigns
    //val columns_keys = Seq("ActivityID", "JourneyID") //Journey
    //val columns_keys = Seq("JobID", "TriggererSendDefinitionObjectID", "TriggeredSendCustomerKey") //Job
    //getSimilarItems(df_or, df_stg, columns_keys).filter(col("SubscriberKey").equalTo("64E698583B998D61A608B6B2BB6014CF")).show(truncate = false)

    /*val namefile = "job.txt"
    writeFormatAnalysisResultsTxt(createFormatAnalysisResultsTxt(df_or, df_stg, columns_keys), "/home/antony/usa/comparative/20201113/new_keys/", namefile)
    */
    //df_or.printSchema()
    //df_stg.printSchema()
    //getSimilarDoubleCheck(getSimilarItems(df_or, df_stg, columns_keys), columns_keys).show()
    printFormatAnalysisResults(createFormatAnalysisResultsTxt(df_or, df_stg, columns_keys))
    //val list_diff = getDiffDFArrayExcEmptyDF(df_or, df_stg, columns_keys)
    //getDiffCountResultDF(df_or, df_stg, columns_keys).show()
    //getIDFromArrayDF(list_diff, columns_keys).show()
    //getIDFromUnivAndSamp(df_or, df_stg, columns_keys).show()
    //print("ID ===>" + getIDFromArrayDF(list_diff, columns_keys).count()) //Get ID
    //list_diff.foreach(df => df.show(truncate = false))
    //val counting: Array[Long] = list_diff.map(df => df.count())
    //renameSimilarColumnsFromArrayDF(list_diff, columns_keys).foreach(df => df.show(truncate = false))
    //getCountDiffResultFromDoubleCheck(doubleCheckDiffArray(list_diff, columns_keys), columns_keys).show()//.foreach(df => df.show(truncate = false))
    //getCountDiffResultFromDoubleCheck(doubleCheckDiffArray(list_diff, columns_keys), columns_keys).foreach(df => df.show(truncate = false))
    //doubleCheckDiffArray(list_diff, columns_keys).map(df => df.show(truncate = false))
    //getSimilarItems(df_or, df_stg, columns_keys).filter(s"SubscriberKey == '5302DF960C56126A23CF2251BB2BBE9D'").show(150,truncate = false)
    //getCountDiffResultFromDoubleCheck(list_diff, columns_keys)
    //getCountDiffResultFromDoubleCheck(double_list, columns_keys).show()
    //val otro = renameSimilarColumnsFromArrayDF(list_diff, columns_keys)
    //otro.foreach(df => df.show(truncate = false))
    //writeArrayDFCSV(otro, "/home/antony/IdeaProjects/comparative_proyect/autocomparative/src/test/resources/output/table_results/", "bounces")
    //joinDFFromArray(getDiffCountIDDF(getCountIDDF(df_or, columns_keys), getCountIDDF(df_stg, columns_keys), columns_keys), columns_keys).show()
    //getDiffCountIDDF(getCountIDDF(df_or, columns_keys), getCountIDDF(df_stg, columns_keys), columns_keys).foreach(df => df.show(truncate = false))
    /*val join: DataFrame = getOppositeItems(df_or, df_stg, columns_keys) //Get anti inner
    //join.show(join.count().toInt, truncate = false)//.select(columns_keys.head).distinct().show()
    val join2: DataFrame = getOppositeItemsSample(df_or, df_stg, columns_keys) //Get similar number of anti inner
    val join3: DataFrame = getOppositeItemsUniverse(df_or, df_stg, columns_keys)
    print("Oppo ===> " + join.count() + "\nOppoRight ===> " + join2.count() + "\nOppoLeft ===> " + join3.count())
    //join.show()
    join3.show(truncate = false)*/
    join2.show(truncate = false)*/
    0
  }

}
