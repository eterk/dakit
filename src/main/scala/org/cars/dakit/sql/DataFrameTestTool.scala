package org.cars.dakit.sql

import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession, SqlVi}

/** dataframe 测试的小工具
 * 部分主要功能集成在DataFrameTool中
 *
 * @author wsk  2021/1/6
 */

object DataFrameTestTool extends Serializable {

  private[this] val (sortCol, nameCol) = ("index", "col_name")


  def compare(dataSeq: Seq[Dataset[_]]): Dataset[_] = {
    compare(dataSeq, dataSeq.indices.map("_" + _))
  }

  import scala.reflect.runtime.universe._

  def getInfo[T <: Product : TypeTag]: Dataset[_] = {
    val spark = SparkSession.active
    import spark.implicits._
    Encoders.
      product[T].schema.
      map(f => (f.name, f.dataType.toString, f.nullable)).
      toDF().
      toDF("name", "data_type", "nullable")
  }


  /** 比较 多个Dataframe的 字段和类型
   *
   * @param dataSeq
   * @param alias
   * @return
   */
  def compare(dataSeq: Seq[Dataset[_]], alias: Seq[String]): Dataset[_] = {
    val size = dataSeq.size
    val dName = (nameCol, "col", "data_type")

    require(size == alias.size)

    val maps: Seq[Map[String, String]] = dataSeq.map(x => x.schema.map(f => (f.name, (f.dataType, f.nullable).toString())).toMap)

    val keys = maps.flatMap(_.keys).distinct


    val values = keys.map(k => (k, maps.map(m => m.getOrElse(k, ""))))

    val results: Seq[(String, Seq[String], String)] = values.map(x => {
      if (x._2.toSet.size == 1) {
        (x._1, List.fill(size)(""), x._2.head)
      } else {
        (x._1, x._2, "")
      }
    })
    val spark = SparkSession.active
    import spark.implicits._
    val select_col = (col(dName._1) +: (0 until size).map(i => col(dName._2)(i).alias(alias(i)))) :+ col(dName._3)
    results.toDF(dName._1, dName._2, dName._3).select(select_col: _*)
  }

  /** 使用反射调用 org.apache.spark.sql.Dataset 里的showString 方法，具体参数请查看 org.apache.spark.sql.Dataset
   */
  def getString(data: Dataset[_], numRows: Int, truncate: Int, vertical: Boolean): String = {
    val ss = data.getClass.getMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    ss.setAccessible(true)
    ss.invoke(data, numRows.asInstanceOf[Object], truncate.asInstanceOf[Object], vertical.asInstanceOf[Object]).asInstanceOf[String]
  }


  /**
   * val (truncate,numRows,vertical)=(20,20,false)
   *
   * @param split md 表格格式中表头和的分割符的哪一行，可以手动加入，以获得需要的对齐效果,split 的间隔 需要和一致data 字段数一致
   *              |：---|  ：左对齐  |：---：| ：居中对齐 |---： 右对齐
   */
  def toMarkDown(data: Dataset[_], split: String = null, numRows: Int = 20, truncate: Int = 20, vertical: Boolean = false): String = {
    if (numRows > 100) {
      println("don't recommend data rows too long")
    }

    val need = data.columns.length + 1

    val actual =
      if (split == null) {
        Array.fill(need)('|').mkString("---")
      } else {
        //      约束输入格式的,未完成，不重要。
        val inputCount = split.count(x => x == '|')
        require(inputCount == need, s"split 的间隔 需要和一致data 字段数一致,需要 $need ,实际 $inputCount 个")
        split
      }


    val str = getString(data, numRows, truncate, vertical).split("\n")

    val head = str.tail.head

    val body = str.tail.drop(2).dropRight(2)

    (Seq(head, actual) ++ body).mkString("\n")
  }

  def getSchemaMD(df: Dataset[_]): String = {
    val spark = SparkSession.active

    import spark.implicits._
    val context = df.schema.fields.map(CaseStructField.apply(_)).toSeq

    val res = spark.createDataset[CaseStructField](context)

    toMarkDown(res.toDF())
  }

  def printSchemaMD(df: Dataset[_]): Unit = {
    getSchemaMD(df)
  }


  private[dakit] case class CaseStructField private(
                                                     field_name: String,
                                                     data_type: String,
                                                     nullable: Boolean,
                                                     tips: String
                                                   )

  object CaseStructField {
    def apply(field: StructField): CaseStructField = new CaseStructField(field.name, field.dataType.simpleString, field.nullable, "")
  }


  def showMarkDown(data: Dataset[_], split: String): Unit = println(toMarkDown(data, split))

  /** 计算 一个dataframe 所有 的na 列
   * 不太建议写入生产代码中，效率太低，仅供分析
   */
  def naCount(data: Dataset[_]): Array[(String, Long)] = {
    def naC(df: Dataset[_], co: String, t: String): (String, Long) = {
      val c =
        if (t == "DoubleType" || t == "FloatType") {
          df.filter(col(co).isNull || col(co).isNaN).count()
        } else {
          df.filter(col(co).isNull).count()
        }
      (co, c)
    }

    data.dtypes.map((x: (String, String)) => naC(data, x._1, x._2)).filter((x: (String, Long)) => x._2 > 0)
  }

  def duplicateCheck(data: Dataset[_], groupCol: Seq[String]): (Boolean, Dataset[_]) = {
    val res = data.groupBy(groupCol.map(col): _*).count().where("count>1")
    (res.isEmpty, res)
  }


  private[this] def caseClassString(name: String, fields: StructType): String = {
    val stringConcat = new StringUtils.StringConcat()

    stringConcat.append(s"case class ${name}(\n")
    val prefix = "    |"
    SqlVi.buildFormattedString(fields, prefix, stringConcat)

    stringConcat.toString()
  }


  private[this] def toCaseClass(schema: StructType, name: String): String = {

    val str = caseClassString(name, schema)


    str.replace("|--", "").
      replace("(nullable = true)", ",").
      replace("(nullable = false)", ",").
      replace("string", "String").
      replace("timestamp", "Timestamp").
      replace("integer", "Int").
      replace("long", "Long").
      replace("float", "Float").
      replace("boolean", "Boolean").
      replace("date", "LocalDate").
      replace("double", "Double").
      replaceAll(",$", ")")
  }


  def printCaseClass(data: Dataset[_], name: String): Unit = {
    println(toCaseClass(data.schema, name))
  }

  def size(df: Dataset[_]) = {
    println("nums of row: " + df.count())
    println("nums of col: " + df.columns.size)
    println("nums of rdd: " + df.rdd.partitions.length)
  }


}
