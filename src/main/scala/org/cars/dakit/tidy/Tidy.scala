package org.cars.dakit.tidy

import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.cars.dakit.sql.DataTypeTool._
import org.cars.dakit.basic.BasicToolSet.allIn

//import com.ttrms.util.dftools.tidy.Tidy._
object Tidy {


  import Unite._


  def structFields(data: DataFrame, colName: String): Seq[StructField] = {
    data.schema(colName).dataType.asInstanceOf[StructType].fields
  }


  def uniteConcat(data: DataFrame, newCol: String, cols: Seq[String], remove: Boolean, sep: Column) = {
    require(cols.map(data.schema(_).dataType).distinct.length == 1, println("all concat column must same type"))
    val uniteCols =
      if (sep == null) {
        cols.map(col)
      } else {
        Unite.insertSep(cols.map(col), sep) // 此处需要判断 这俩类型一致，但是在这里暂时没做判断 否则会报错
      }

    val result = ConcatUnite().unite(data, newCol, uniteCols) // concat  要求所有类型都要一致
    if (remove) {
      result.drop(cols: _*)
    } else {
      result
    }
  }

  def uniteConcat(data: DataFrame, newCols: Seq[String], cols: Seq[Seq[String]], remove: Boolean, sep: Column) = {
    check(data: DataFrame, newCols: Seq[String], cols: Seq[Seq[String]])

    val uniteCols: Seq[Seq[Column]] =
      if (sep == null) {
        cols.map(_.map(col))
      } else {
        cols.map(c => Unite.insertSep(c.map(col), sep))
      }
    val result = ConcatUnite().unites(data, newCols, uniteCols) // concat  要求所有类型都要一致

    if (remove) {
      result.drop(cols.flatten: _*)
    } else {
      result
    }
  }

  private def check(data: DataFrame, newCols: Seq[String], cols: Seq[Seq[String]]): Unit = {
    require(newCols.size == cols.size, println("check input params"))
    require(allIn(data.columns, cols.flatten), println("all col must in data " + cols.diff(data.columns).mkString(" , ")))
  }


  /** 将一些列struct 为新的列
   * {{{
   *   newCol 存在于cols 中会出问题
   *
   * }}}
   *
   * @param newCol 新列列名
   * @param cols   需要合并的行
   * @param remove 是否溢出原来的行
   */
  def unite(data: DataFrame, newCol: String, cols: Seq[String], remove: Boolean): DataFrame = {
    require(allIn(data.columns, cols), println("all col must in data " + cols.diff(data.columns).mkString(" , ")))

    val result = StructUnite().unite(data, newCol, cols.map(col))
    if (remove) {
      result.drop(cols: _*)
    } else {
      result
    }
  }


  /** 将多个列组合 合并
   *
   * @param newCols 新的列名
   * @param cols    这些新列的组成成员
   * @param remove  受否移除原列
   */
  def unite(data: DataFrame, newCols: Seq[String], cols: Seq[Seq[String]], remove: Boolean): DataFrame = {
    check(data: DataFrame, newCols: Seq[String], cols: Seq[Seq[String]])

    val a: Unite = StructUnite()
    val result = a.unites(data, newCols, cols.map(_.map(col)))
    if (remove) {
      result.drop(cols.flatten: _*)
    } else {
      result
    }
  }

  // 不会写截取字符串的正则表达是 暂时替代  最多取到二级
  private val get_two = (s: String) => {
    val sp = s.split("\\.")
    if (sp.length > 2) {
      sp(0) + "." + sp(1)
    } else {
      s
    }
  }

  private def colGet(columns: Seq[String], colName: String): Seq[String] = {
    val reg = ("^" + colName + "\\.").r
    columns.filter(s => reg.findFirstIn(s).isDefined)
  }


  def separate(data: Dataset[_], remove: Boolean, recursive: Boolean): DataFrame = {
    val cond = data.columns.filter(colCheck[StructType](data, _))
    separate(data, cond, remove, recursive)
  }


  /**
   *
   * @param sepCol 需要切割的列
   * @param select 需要选择的子列 （目前不支持二级以下）
   */
  def separate(data: DataFrame, sepCol: String, select: Seq[String], remove: Boolean): DataFrame = {
    val cond = colCheck[StructType](data, sepCol)

    require(cond, s"${sepCol}must exists and StructType")

    val origin_col: Seq[String] = data.columns

    val index = origin_col.indexOf(sepCol)
    val new_col = select.map(sepCol + "." + _)

    val result_col =
      if (remove) {
        origin_col.take(index) ++ new_col ++ origin_col.takeRight(origin_col.size - 1 - index)
      } else {
        origin_col.take(index + 1) ++ new_col ++ origin_col.takeRight(origin_col.size - 1 - index)
      }

    data.select(result_col.map(col): _*)
  }


  def separate(data: Dataset[_], cols: Seq[String], remove: Boolean, recursive: Boolean): DataFrame = {
    val cond = cols.filter(!colCheck[StructType](data, _))
    //todo  添加 要求cols 必须存在data 中的断言

    require(cond.isEmpty, cond.foreach(println(_, "must exists and StructType")))

    var columns = flatSchemaName(data.schema)

    if (!recursive) {
      columns = columns.map(get_two).distinct
    }

    val result_col: Array[String] = data.columns.flatMap(col => {
      if (cols.contains(col)) {
        if (!remove) {
          col +: colGet(columns, col)
        } else {
          colGet(columns, col)
        }: Seq[String]
      } else {
        Seq(col)
      }
    })

    data.select(result_col.map(col): _*)
  }

  def flatSchemaName(schema: StructType, prefix: String = null): Seq[String] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else prefix + "." + f.name
      f.dataType match {
        case st: StructType => flatSchemaName(st, colName)
        case _ => Seq(colName)
      }
    })
  }


  /** * 将同一类型的一些数据合并为同一列
   *
   * @param by    同一类型 的数据
   * @param key   分组的列
   * @param value 结果列
   */
  def gather(data: DataFrame, by: Seq[String], key: String, value: String): DataFrame = {
    val c_d = data.dtypes.filter(x => !by.contains(x._1))
    val (cols, dtypes) = c_d.unzip
    assert(dtypes.distinct.length == 1, "All columns have to be of the same type  \n" + c_d.map(x => x._1 + ":" + x._2).mkString("   "))
    val col_seq = cols.map(x => struct(lit(x).alias(key), col(x).alias(value)))
    val kvs = explode(array(col_seq: _*)).alias("kvs")
    data.select(by.map(col) :+ kvs: _*).
      select((by ++ Seq("kvs." + key, "kvs." + value)).map(col): _*)
  }


  def flatSchema(schema: StructType, prefix: String = null): Seq[Column] = flatSchemaName(schema, prefix).map(col(_))
}
