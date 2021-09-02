package com.anhb.comparative.spark.functions

//import org.apache.spark.sql.expressions.Window
import com.anhb.comparative.spark.functions.rules.RulesComparativeFunc

import java.io.{BufferedWriter, File, FileWriter}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc_nulls_last, col, count, expr, lag, lit, monotonically_increasing_id, sum, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/**
  * SparkComparativeFunc is the object that contains functionalities to compare two dataframes equals
  */

object SparkComparativeFunc extends RulesComparativeFunc with GenericFunc {

  lazy val join_fullouter: String = "fullouter"
  lazy val join_inner: String = "inner"
  lazy val join_right: String = "right"
  lazy val join_left: String = "left"
  lazy val row_number_col: String = "row_number"
  lazy val id_col: String = "ID"

  /**
    * getSimilarItems method gets information exists from the sample in the universe
    * | columns from Universe | columns from Sample
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with the ID columns
    * @param distinct Optional argument to distinct dataframes
    * @return
    */
  def getSimilarItems(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean=false): DataFrame = {
    if(distinct){df_universe.as(alias_univ).distinct().join(df_sample.as(alias_sample).distinct(), id_col_seq, join_inner)}
    else{df_universe.as(alias_univ).join(df_sample.as(alias_sample), id_col_seq, join_inner)}
  }

  /**
    * getOppositeItems method gets information not exists from the sample in the universe
    * | columns from Universe | columns from Sample
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with the ID columns
    * @return
    */
  def getOppositeItems(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String]): DataFrame = {
    df_universe.as(alias_univ).join(df_sample.as(alias_sample), id_col_seq, join_fullouter).where(ruleOppositeWhere(id_col_seq))
  }

  def getOppositeItemsSample(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String]): DataFrame = {
    df_universe.as(alias_univ).join(df_sample.as(alias_sample), id_col_seq, join_right).where(ruleOpossiteWhereRight(id_col_seq))
  }

  def getOppositeItemsUniverse(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String]): DataFrame = {
    df_universe.as(alias_univ).join(df_sample.as(alias_sample), id_col_seq, join_left).where(ruleOpossiteWhereLeft(id_col_seq))
  }

  /**
    * getDiffDFArrayForEachColumn method returns the difference validation and separates each column by Dataframe;
    * each one includes ID, column from universe, column from sample and the flag if both columns are equal
    * @param df_similar Dataframe from getSimilarItems method
    * @param array_column Array with column names excluding ID's columns
    * @param id_col_seq Seq with ID columns
    * @return
    */
  def getDiffDFArrayForEachColumn(df_similar: DataFrame, array_column: Array[String], id_col_seq: Seq[String]): Array[Dataset[Row]] = {
    array_column.map(colu => {
      val sel_cols: Array[Column] = id_col_seq.map(e => col(e)).toArray ++ Array(col(s"$alias_univ.$colu")) ++
        Array(col(s"$alias_sample.$colu")) ++ Array(ruleDiffValidation(colu))
      df_similar.select(sel_cols:_*)
    })
  }


  /**
    * getDiffCountResultDF returns a dataframe that contains the total differences number for each column
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with ID columns
    * @param distinct Optional argument to distinct dataframes
    * @return
    */
  def getDiffCountResultDF(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean=false): DataFrame ={
    val diff_df: DataFrame = applySentencesFromArray(
      getSimilarItems(df_universe, df_sample, id_col_seq, distinct),
      ruleDiffValidationFromArray(getArraySchemaExcID(df_universe, id_col_seq))
    )
    applySumMapAggToWholeCol(diff_df, getMapSumSentencesFromDF(diff_df))
  }

  /**
    * getDiffCountResultDFByRecord returns a dataframe that contains the difference column flag for each ID
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with ID columns
    * @param distinct Optional argument to distinct dataframes
    * @return
    */
  def getDiffCountResultDFByRecord(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean=false): DataFrame ={
    val diff_df = applySentencesFromArrayWithID(
      getSimilarItems(df_universe, df_sample, id_col_seq, distinct), id_col_seq,
      ruleDiffValidationFromArray(getArraySchemaExcID(df_universe, id_col_seq)))
    val schema_res: Array[Column] = getArraySchemaExcID(diff_df, id_col_seq).map(colu => sum(colu).as(colu))
    diff_df.groupBy(id_col_seq.map(e => col(e)):_*).agg(schema_res.head, schema_res.tail:_*)
  }

  /**
    * getDiffDFArrayExcEmptyDF returns a list of dataframes that only contains the real differences excluding empty dataframes
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with ID columns
    * @param distinct Optional argument to distinct dataframes
    * @return
    */
  def getDiffDFArrayExcEmptyDF(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean=false): Array[Dataset[Row]] ={
    excludeEmptyDFFromArray(getDiffDFArrayForEachColumn(getSimilarItems(df_universe, df_sample, id_col_seq, distinct),
      getArraySchemaExcID(df_universe, id_col_seq), id_col_seq).map( df => { val flag_col: String = df.schema.last.name
      df.filter(col(df.schema.last.name).equalTo(1)).drop(flag_col)})
    )
  }

  /**
    * getDiffResultDF returns a dataframe that only contains the real differences
    * @param df_universe Dataframe with the whole real content
    * @param df_sample Dataframe with a sample of the information to compare against the universe
    * @param id_col_seq Seq with ID columns
    * @param distinct Optional argument to distinct dataframes
    * @return
    */ /*fix this since has used outer join instead of full_outer*/
  def getDiffResultDF(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean=false): DataFrame ={
    joinDFFromArray(getDiffDFArrayExcEmptyDF(df_universe, df_sample, id_col_seq, distinct), id_col_seq)
  }

  def getIDFromArrayDF(df_array: Array[Dataset[Row]], id_col_seq: Seq[String]): DataFrame = {
    unionDFFromArray(df_array.map(df => df.select(id_col_seq.head, id_col_seq.tail:_*))).distinct()
  }

  def getIDFromUnivAndSamp(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String]): DataFrame = {
    getOppositeItems(df_universe, df_sample, id_col_seq).select(id_col_seq.head, id_col_seq.tail:_*).distinct()
  }

  def renameSimilarColumnsFromArrayDF(df_array: Array[Dataset[Row]], id_col_seq: Seq[String]): Array[Dataset[Row]] = {
    df_array.map( df => { val name: String = df.schema.last.name
      df.selectExpr(id_col_seq ++ Seq(s"$alias_univ.$name as Univ_$name", s"$alias_sample.$name as Samp_$name"):_*)
    })
  }

  def createFormatAnalysisResultsTxt(df_universe: DataFrame, df_sample: DataFrame, id_col_seq: Seq[String], distinct: Boolean = false): Array[String] = {
    lazy val head_doc: String = "=============Results============="
    lazy val tail_doc: String = "================================="
    lazy val count_universe: String = df_universe.count().toString
    lazy val count_sample: String = df_sample.count().toString
    lazy val count_similar: String = getSimilarItems(df_universe, df_sample, id_col_seq, distinct).count().toString
    val diff_col: DataFrame = getDiffCountResultDF(df_universe, df_sample, id_col_seq, distinct)
    lazy val column_names: Array[String] = diff_col.columns.map(
      col => col.substring(4).replace("_res)", "")
    )
    lazy val column_values: Array[String] = diff_col.first.toSeq.map(_.toString).toArray
    val head_content: Array[String] = Array(
      head_doc, s"Universe: $count_universe", s"Sample: $count_sample", s"Similar: $count_similar", tail_doc
    )
    val tail_content: Array[String] = column_names.zipWithIndex.map({ case (col_name, i) =>
      col_name + ": " + column_values(i)
    }) ++ Array(tail_doc)
    head_content ++ tail_content
  }

  def writeFormatAnalysisResultsTxt(format_analy_res: Array[String], path: String, name_file: String = "analysis_result.txt"):Unit = {
    val file = new File(path + name_file)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- format_analy_res){
      bw.write(line + "\n")
    }
    bw.close()
  }

  def writeArrayDFCSV(df_array: Array[Dataset[Row]], path: String, name_directory: String = "differences", savemode: String = "overwrite",
                   repartition: Int = 1, header: Boolean = true, delimiter: String = "|"): Unit = {
    for(i <- df_array.indices){
      df_array(i).repartition(repartition).write.mode(savemode).option("header", header).option("delimiter", delimiter).csv(path + name_directory + i.toString)
    }
  }

  def convertSeqStrToSeqCol(id_column: Seq[String]): Seq[Column] = {
    id_column.map(name => col(name))
  }

  def getCountIDDF(df: DataFrame, id_column: Seq[String]): DataFrame = {
    val schema_res: Array[Column] = id_column.map(column => count(col(column)).as(column + "_count")).toArray
    df.groupBy(convertSeqStrToSeqCol(id_column):_*).agg(schema_res.head, schema_res.tail:_*)
  }

  def getDiffCountIDDF(df_universe_count: DataFrame, df_sample_count: DataFrame, id_column: Seq[String]): Array[Dataset[Row]] ={
    getDiffDFArrayExcEmptyDF(df_universe_count, df_sample_count, id_column)
  }
  /*df_array.map( df => {
    val name: String = df.select(s"$alias_univ.*").schema.last.name
    val df_univ = df.select(s"$alias_univ.*").withColumnRenamed(s"$name", s"univ_$name")
    val df_samp = df.select(s"$alias_sample.*", id_col_seq:_*).withColumnRenamed(s"$name", s"samp_$name")
    df_univ.join(df_samp, id_col_seq, join_inner)
  })*/

  /* val diff_df: DataFrame = applySentencesFromArray(
      getSimilarItems(df_universe, df_sample, id_col_seq, distinct),
      ruleDiffValidationFromArray(getArraySchemaExcID(df_universe, id_col_seq))
    )
    applySumMapAggToWholeCol(diff_df, getMapSumSentencesFromDF(diff_df), id_col_seq)
  * */

  /*def getDoubleCheckDF(df_universe: DataFrame, df_sample: DataFrame, id_columns: Seq[String]): DataFrame = {
    val list_df_universe: Array[Dataset[Row]] = df_universe.drop(id_columns:_*).schema.fieldNames.map( name =>
      df_universe.selectExpr(id_columns ++ Seq(s"$alias_univ.$name"):_*).orderBy(asc_nulls_last(name)).withColumn(row_number_col, monotonically_increasing_id())
    )
    val list_df_sampe: Array[Dataset[Row]] = df_sample.drop(id_columns:_*).schema.fieldNames.map( name =>
      df_sample.selectExpr(id_columns ++ Seq(s"$alias_sample.$name"):_*).orderBy(asc_nulls_last(name)).withColumn(row_number_col, monotonically_increasing_id())
    )
    //getSimilarItems(df_universe, df_sample, id_columns)
  }*/


  def doubleCheckDiffArray(list_df: Array[Dataset[Row]], id_col_seq: Seq[String]): Array[Dataset[Row]] = {
    list_df.map( df =>{
      val name: String = df.schema.last.name
      val df_univ = df.select(s"$alias_univ.*").orderBy(asc_nulls_last(name)).withColumn(row_number_col, monotonically_increasing_id())
      val df_samp = df.select(s"$alias_sample.*", id_col_seq:_*).orderBy(asc_nulls_last(name)).withColumn(row_number_col, monotonically_increasing_id())
      df_univ.join(df_samp, Seq(row_number_col) ++ id_col_seq, join_inner).drop(row_number_col)
        .selectExpr(id_col_seq ++ Seq(s"$alias_univ.$name", s"$alias_sample.$name", ruleDiffValidationDoubleCheck(name)):_*)
    })
  }

  /*Reaname as getTotalResultByID*/
  def getCountDiffResultFromDoubleCheck(list_df_doublecheck: Array[Dataset[Row]], id_col_seq: Seq[String]): DataFrame = {
    joinDFFromArray(list_df_doublecheck.map(df => df.selectExpr(s"1 as $id_col", df.schema.last.name).groupBy(id_col)
      .agg(sum(df.schema.last.name))), Seq(id_col)).drop(id_col)

    //val real_count_diff: Array[Map[String, Long]] = list_df.map(df => Map(df.schema.last.name -> df.count()))
    /*val new_count_diff: Array[Map[String, Long]] = doubleCheckDiffArray(list_df, id_col_seq).map(df => Map(df.schema.last.name -> df.count()))
    //real_count_diff.foreach(k => println(k))
    new_count_diff.foreach(k => println(k))
    new_count_diff*/
    /*
    list_df.map( df => {
      val name: String = df.schema.last.name
      /*val id_col: Seq[String] = id_col_seq ++ Seq(s"$alias_univ.$name", s"$alias_sample.$name", ruleDiffValidationDoubleCheck(name))
      //df.select(Seq(col(s"$alias_univ.$name")) ++ Seq(col(s"$alias_sample.$name")) ++ Seq(ruleDiffValidationDoubleCheck(name)) ++ id_col_seq.map(colu => col(colu)):_*)
      df.selectExpr(id_col:_*)*/
      //df.select(ruleDiffValidationDoubleCheck(name))
      Map( name -> df.selectExpr(ruleDiffValidationDoubleCheck(name)).agg(sum(name + "_res")).head.getLong(0))
    })*/
  }

  def getSimilarDoubleCheck(get_similar_df :DataFrame, id_col_seq: Seq[String]): DataFrame ={

    val expr: Seq[Column] = getArraySchemaExcID(get_similar_df, id_col_seq).map( column_name =>
      when(col(s"$alias_univ.$column_name") === col(s"$alias_sample.$column_name").over(Window.orderBy(asc_nulls_last(s"$alias_univ.$column_name"))) || (
        col(s"$alias_univ.$column_name").isNull && col(s"$alias_sample.$column_name").over(Window.orderBy(asc_nulls_last(s"$alias_univ.$column_name"))).isNull)
        , 0).otherwise(1).as(column_name + "_res")
    )
    val schema: Seq[String] = getArraySchemaExcID(get_similar_df, id_col_seq)
    get_similar_df.select(id_col_seq.map(e => col(e)) ++ schema.map(e => col(s"$alias_univ.$e")) ++ schema.map(e => col(s"$alias_sample.$e")) ++ expr:_*)
  }


  def ruleAccuracyResults(col_name: String, id_column: Seq[String], precission_rows: Int = 1): Column ={
    print(col_name)
    when(
      col(s"$alias_sample.$col_name") === lag(s"$alias_univ.$col_name", precission_rows).over(Window.orderBy(id_column(0))) ||
        (col(s"$alias_sample.$col_name").isNull && col(s"$alias_univ.$col_name").isNull),0
    ).otherwise(1).as(col_name + "_press")
  }

  def getPrescisionResults(schema: Array[String], id_column: String, precission_rows: Int = 1): Array[Column] ={
    schema.map( col_name =>
      when(
        col(s"$col_name") === lag(s"$col_name", precission_rows).over(Window.orderBy(id_column)) ||
          (col(s"$col_name").isNull && col(s"$col_name").isNull),0
      ).otherwise(1).as(col_name + "_pres")
    )
  }

  def makePrecisionResults(list_df: Array[Dataset[Row]], id_column: Seq[String], lag_pre: Int):Array[Dataset[Row]] = {
    list_df.map( df => {
      df.show(truncate = false)
      df.printSchema()
      print("0: " + df.columns(0) + "\n1: " + df.columns(1) + "\n2:" + df.columns(2))
      df
      //df.select(id_column.map(e => col(e)):_*, ruleAccuracyResults(df.columns(2), id_column, lag_pre))
      //df.withColumn(df.columns(2).toString + "_press", ruleAccuracyResults(df.columns(2), id_column))
    })//.withColumn("Precission", getPrescisionResultsList(df.columns(2), id_column))//.select( "Precission")
      //.agg(Map("Precission" -> "sum"))
  }

  /*
  def makePrecisionResults(df: DataFrame, id_column: String, precission_rows: Int = 1):DataFrame = {
    df.show()
    val schema: Array[String] = getArraySchemaWithoutID(df, id_column)
    df.select(getPrescisionResults(schema, id_column, precission_rows):_*)//.select( "Precission")
      //.agg(Map("Precission" -> "sum"))
  }*/


}
