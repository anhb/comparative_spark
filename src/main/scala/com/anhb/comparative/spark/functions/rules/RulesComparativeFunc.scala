package com.anhb.comparative.spark.functions.rules

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when}

abstract class RulesComparativeFunc {

  protected val alias_univ: String = "u"
  protected val alias_sample: String = "s"


  def ruleDiffValidation(column_name: String): Column = {
    when(col(s"$alias_univ.$column_name") === col(s"$alias_sample.$column_name") || (
      col(s"$alias_univ.$column_name").isNull && col(s"$alias_sample.$column_name").isNull)
      , 0).otherwise(1).as(column_name + "_res")
  }

  def ruleDiffValidationFromArray(array_columns: Array[String]): Array[Column] = {
    array_columns.map(column => {
      when(col(s"$alias_univ.$column") === col(s"$alias_sample.$column") || (
        col(s"$alias_univ.$column").isNull && col(s"$alias_sample.$column").isNull)
        , 0).otherwise(1).as(column + "_res")
    })
  }

  def ruleOppositeWhere(id_col_seq: Seq[String]): String ={
    if(id_col_seq.length == 1){
      id_col_seq.map(column =>
        col(s"$alias_univ.$column").isNull || col(s"$alias_sample.$column").isNull
      ).head.toString()
    }
    else{
      id_col_seq.map(column =>
        col(s"$alias_univ.$column").isNull || col(s"$alias_sample.$column").isNull
      ).mkString(" AND ")
    }
  }

  def ruleOpossiteWhereRight(id_col_seq: Seq[String]): String ={
    if(id_col_seq.length == 1){
      id_col_seq.map(column =>
        col(s"$alias_univ.$column").isNull
      ).head.toString()
    }else{
      id_col_seq.map(column =>
        col(s"$alias_univ.$column").isNull
      ).mkString(" AND ")
    }
  }

  def ruleOpossiteWhereLeft(id_col_seq: Seq[String]): String ={
    if(id_col_seq.length == 1){
      id_col_seq.map(column =>
        col(s"$alias_sample.$column").isNull
      ).head.toString()
    }else{
      id_col_seq.map(column =>
        col(s"$alias_sample.$column").isNull
      ).mkString(" AND ")
    }
  }

  def ruleDiffValidationDoubleCheck(column_name: String): String = {
    when(col(s"$alias_univ.$column_name") === col(s"$alias_sample.$column_name") || (
      col(s"$alias_univ.$column_name").isNull && col(s"$alias_sample.$column_name").isNull)
      , 0).otherwise(1).as(column_name + "_res").toString()
  }


}