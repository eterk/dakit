package org.cars.dakit.sql

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.cars.dakit.basic.BasicToolSet.{allIn, filedNames}
import org.cars.dakit.basic.C
import org.slf4j.LoggerFactory

import scala.annotation.{tailrec, varargs}

object DataFrameBasic {
  private val spark = SparkSession.active
  val logger = LoggerFactory.getLogger(getClass.getName)

  //todo 对不存在的列名进行提醒
  /**
   * 效率非常低
   *
   * @return
   */
  @tailrec
  def rename1(df: Dataset[_], exists: Seq[String], news: Seq[String]): Dataset[_] = {
    if (exists.isEmpty) return df
    rename1(df.withColumnRenamed(exists.head, news.head), exists.tail, news.tail)
  }

  /**
   * @param colNames 选择的列名
   * @param strict   是否严格限制（false 时即使列不存在也不报错）
   */
  def select(data: Dataset[_], colNames: Seq[String], strict: Boolean): Dataset[_] = {
    require(allIn(data.columns, colNames) | (!strict), {
      s"\r\n provide :${data.columns.mkString(",")} \r\n need:  ${colNames.mkString(",")},\r\n lack:${colNames.diff(data.columns).mkString(",")}"
    })
    val cols = colNames.intersect(data.columns).map(col)
    data.select(cols: _*)
  }

  // 尚未开发完
  def select(data: Dataset[_], colNames: Any*): Dataset[_] = {

    val r: Seq[Seq[String]] =
      colNames.map(x => x match {
        case Symbol("other") => data.columns.toSeq
        case Symbol("reverse") => data.columns.reverse.toSeq
        case str: String => Seq(str)
        case _ => throw new IllegalArgumentException("illegal arg parse")
      })

    val op = (x: Seq[String], a: Seq[String]) => C(x, a.filterNot(x.contains(_)))

    val selected = r.foldLeft(Seq[String]())(op).map(col)


    data.select(selected: _*)
  }

  import scala.reflect.runtime.universe.TypeTag

  @Deprecated
  def toSimple[T <: Product : TypeTag](data: Dataset[_]): Dataset[T] = {
    get[T](data).as[T](Encoders.product[T])
  }

  /**
   * 带有选列，强制转换的类型，暂时nullable  无法保证
   *
   * @param data 数据
   * @tparam T 需要转换的类型
   */
  def to[T <: Product : TypeTag](data: Dataset[_]): Dataset[T] = {
   SmartConverter.productConvert[T](data)
  }


  def get[T <: Product : TypeTag](data: Dataset[_]): Dataset[_] = {
    select(data, filedNames[T], true)
  }


  /**
   * @param dataSeq  需要join
   * @param joinType DataFrame 中join 的类型都支持
   * @return 执行完成后的数据
   */
  def autoJoinByCommonCol(dataSeq: Seq[Dataset[_]], joinType: String): Dataset[_] = {
    val s: Seq[Wrap] = dataSeq.map(new Wrap(_, joinType))
    s.reduce(_ union _).get
  }


  @varargs
  def autoJoinByCommonCol(joinType: String, dataSeq: Dataset[_]*): Dataset[_] = {
    autoJoinByCommonCol(dataSeq, joinType)
  }

  /**
   * 详见 {{{autoJoinByCommonCol}}}
   */
  private class Wrap(val data: Dataset[_], val joinType: String) {
    def get(): Dataset[_] = data

    def union(dataR: Wrap): Wrap = {
      val joinCols = data.columns.intersect(dataR.data.columns)
      // TODO: 应该把join 的列输出到日志中或者提示 ,否则同名不同意的列会造成bug
      logger.info(s"use join cols ${joinCols.mkString(",")} ;")
      new Wrap(data.join(dataR.data, joinCols, joinType), joinType)
    }
  }


  def withColumns(data: Dataset[_], map: Map[String, Column]): Dataset[_] = {
    withColumns(data, map.toSeq)
  }

  /** 插入 map 格式的列
   */
  @tailrec
  final def withColumns(data: Dataset[_], seq: Seq[(String, Column)]): Dataset[_] = {

    if (seq.isEmpty) {
      data
    } else {

      withColumns(data.withColumn(seq.head._1, seq.head._2), seq.tail)
    }
  }


  /**
   * 对多列应用一个函数,最终返回这个函数的结果
   */
  def withColumns(data: Dataset[_], colNames: Seq[String], f: Column => Column, newColNames: Seq[String] = Nil): Dataset[_] = {

    val col_new_name =
      if (newColNames.isEmpty) {
        colNames.zip(colNames)
      } else {
        require(newColNames.size == colNames.size, "列数必须一致")
        colNames.zip(newColNames)
      }
    withColumns(data, col_new_name.map(c => (c._2, f(col(c._1)))))
  }

  def withColumns(data: Dataset[_], colNames: Seq[String], f: Column => Column, prefix: String): Dataset[_] = {
    withColumns(data, colNames, f, colNames.map(prefix + "_" + _))
  }


  def rename(df: Dataset[_], existName: Seq[String], prefix: String, suffix: String): Dataset[_] = {

    val newNames = existName.map(Seq(prefix, _, suffix).filterNot("" == _).mkString("_"))

    rename(df, existName, newNames)

  }

  // 实现1
  def rename(df: Dataset[_],
             existName: Seq[String],
             newNames: Seq[String]): Dataset[_] = {
    val resolver = spark.sessionState.analyzer.resolver
    val output = df.queryExecution.analyzed.output

    val isExist = (o: Attribute) => existName.indexWhere(resolver(o.name, _))
    val cols = output.map(o => {
      val index = isExist(o)
      if (index == -1) {
        new Column(o)
      } else {
        new Column(o).as(newNames(index))
      }
    })

    df.select(cols: _*)
  }

  def withColumns2(data: Dataset[_], seq: Seq[(String, Column)]): Dataset[_] = {
    val name = seq.unzip._1
    require(data.columns.diff(name).size == data.columns.size, "x") //todo 暂时无法处理
    val cols = seq.unzip._2
    withColumns2(data, name, cols)
  }


  // 没测试好,有问题
  private def withColumns2(df: Dataset[_], colNames: Seq[String], cols: Seq[Column]): Dataset[_] = {
    require(colNames.size == cols.size,
      s"The size of column names: ${colNames.size} isn't equal to " +
        s"the size of columns: ${cols.size}")
    //                SchemaUtils.checkColumnNameDuplication(
    //                  colNames,
    //                  "in given column names",
    //                  sparkSession.sessionState.conf.caseSensitiveAnalysis)

    val resolver = spark.sessionState.analyzer.resolver
    val output = df.queryExecution.analyzed.output

    val columnMap = colNames.zip(cols).toMap

    val replacedAndExistingColumns = output.map { field =>
      columnMap.find { case (colName, _) =>
        resolver(field.name, colName)
      } match {
        case Some((colName: String, col: Column)) => col.as(colName)
        case _ => new Column(field)
      }
    }

    val newColumns: Seq[Column] = columnMap.filter { case (colName, c) =>
      !output.exists(f => resolver(f.name, colName))
    }.map { case (colName, col) => col.as(colName) }.toSeq
    //    import org.apache.spark.sql.functions.lit
    val init = df.select(replacedAndExistingColumns: _*)


    se(init, replacedAndExistingColumns, newColumns)

  }

  @tailrec
  final private def se(df: Dataset[_], exist: Seq[Column], append: Seq[Column]): Dataset[_] = {
    if (append.isEmpty) return df
    val newE = exist ++ Seq(append.head)
    se(df.select(newE: _*), newE, append.tail)
  }


}
