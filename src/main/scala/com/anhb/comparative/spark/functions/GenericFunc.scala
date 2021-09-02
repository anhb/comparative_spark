package com.anhb.comparative.spark.functions

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

trait GenericFunc {

  /**
    * getArraySchema method gets schema from dataframe in an Array
    * @param df
    * @return
    */
  def getArraySchema(df: DataFrame): Array[String] = df.schema.fields.map(_.name)

  /**
    * getArraySchemaExcID gets schema from dataframe excluding ID in a Seq
    * @param df
    * @param id_col_seq
    * @return
    */
  def getArraySchemaExcID(df: DataFrame, id_col_seq: Seq[String]): Array[String] = df.drop(id_col_seq:_*).schema.fields.map(_.name).toSet.toSeq.toArray


  /**
    * getMapSumSentencesFromDF method makes a Map, and this one inserts the aggregation method for each column, example:
    * column_name -> sum
    * @param df
    * @return
    */
  def getMapSumSentencesFromDF(df: DataFrame): Map[String, String] = Map(getArraySchema(df) map { key => (key, "sum") }:_*)

  /**
    * applySentencesFromArray method applies the validation using the SQL sentences from an Array
    * @param df
    * @param sentence_expr
    * @return
    */
  def applySentencesFromArray(df: DataFrame, sentence_expr: Array[Column]): DataFrame = df.select(sentence_expr:_*)

  /**
    * applySentencesFromArrayWithID method applies the validation using the SQL sentences from an Array and this one includes the ID's
    * @param df
    * @param id_col_seq
    * @param sentence_expr
    * @return
    */
  def applySentencesFromArrayWithID(df: DataFrame, id_col_seq: Seq[String], sentence_expr: Array[Column]): DataFrame = {
    df.select(id_col_seq.map(e => col(e)).toArray ++ sentence_expr:_*)
  }

  /**
    * applySumMapAggToWholeCol method does the sum aggregation for the whole columns using a Map Variable
    * @param df
    * @param sentence_sum
    * @return
    */
  def applySumMapAggToWholeCol(df: DataFrame, sentence_sum: Map[String, String]): DataFrame = {
    df.agg(sentence_sum)
  }

  /**
    * joinDFFromArray method joins the whole dataframes from an Array
    * @param df_array
    * @param id_col_seq
    * @param join_type
    * @return
    */
  def joinDFFromArray(df_array: Array[Dataset[Row]], id_col_seq: Seq[String], join_type: String = "outer"): DataFrame = {
    df_array reduce(_.join(_, id_col_seq, join_type))
  }

  /**
    * unionDFFromArray method union the whole dataframes from an array
    * @param df_array
    * @return
    */
  def unionDFFromArray(df_array: Array[Dataset[Row]]): DataFrame = df_array reduce(_.union(_))

  /**
    * excludeEmptyDFFromArray excludes empty dataframes from an Array
    * @param df_array
    * @return
    */
  def excludeEmptyDFFromArray(df_array: Array[Dataset[Row]]): Array[Dataset[Row]] = {
    df_array.map(df => {if(!df.rdd.isEmpty()) df else false}).filter(!_.equals(false)).map(e => e.asInstanceOf[Dataset[Row]])
  }


}
