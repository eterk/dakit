package org.cars.dakit

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.cars.dakit.basic.BasicToolSet
import org.cars.dakit.sql.{DataFrameBasic, DataFrameTestTool, FilterTool, GroupSample}
import org.cars.dakit.tidy.Tidy
import org.cars.dakit.basic.{C => Combine}
import org.cars.dakit.sql.{ColumnSet=>C_Set}
import scala.annotation.varargs

object utils {

  /**
   * 组合单个元素和集合
   * {{{
   *   C("train_code",Seq("start_date","stop_date"))
   *   > Seq("train_code","start_date","stop_date"))  :Seq[String]
   *   C(3,Seq(3,4,5),Seq(2,1))
   *   > Seq(3,3,4,5,2,1)) :Seq[Int]
   * }}}
   *
   */
  val C = Combine

  val Basic = DataFrameBasic


  /**
   * 提供一些 测试开发中的工具
   */
  val Test = DataFrameTestTool

  /** *
   * 一些常用的数据列
   */
  val ColumnSet = C_Set

  /**
   * 一些dataframe 的 隐式方法
   * {{
   * import Utils.DataFrameUtil.implicits._
   *
   * }}
   * 如果有bug 及时反馈
   */
  implicit class implicits(data: Dataset[_]) {

    /**
     * @param colNames 选择的列名
     * @param strict   是否严格限制（false 时即使列不存在也不报错）
     */
    def select(colNames: Seq[String], strict: Boolean): Dataset[_] = Basic.select(data, colNames, strict)

    /** 根据列名选择列
     */
    def select(colNames: Seq[String]): Dataset[_] = Basic.select(data, colNames, false)


    def repartitionAndSort(repartitionCols: Seq[String], sortCols: Seq[String]): Dataset[_] = {
      data.repartition(repartitionCols.map(col(_)): _*)
        .sortWithinPartitions(sortCols.head, sortCols.tail: _*)
    }

    def colExist(colNames: Seq[String]): Boolean = {
      BasicToolSet.allIn(data.columns, colNames)
    }

    /** 一次性插入多列
     *
     * @param map 列名和列的映射
     * @return
     */
    def withColumns(map: Map[String, Column]): Dataset[_] = Basic.withColumns(data, map)

    def withColumns(map: Seq[(String, Column)]): Dataset[_] = Basic.withColumns(data, map)

    def withColumns(colNames: Seq[String], f: Column => Column): Dataset[_] = Basic.withColumns(data, colNames, f)

    def withColumns(colNames: Seq[String], f: Column => Column, newNames: Seq[String]): Dataset[_] = Basic.withColumns(data, colNames, f, newNames)

    def withColumns(colNames: Seq[String], f: Column => Column, prefix: String): Dataset[_] = Basic.withColumns(data, colNames, f, prefix)


    def filters(fMap: Map[String, Object]): Dataset[_] = {
      FilterTool.filters(data.toDF(), fMap, false)
    }

    def cacheFilters(fMap: Map[String, Object]): Dataset[_] = {
      FilterTool.filters(data.toDF(), fMap, true)
    }

    def filters(fMap: Map[String, Object], cache: Boolean): Dataset[_] = {
      FilterTool.filters(data.toDF(), fMap, cache)
    }


    import scala.reflect.runtime.universe.TypeTag

    /** 将dataframe 转化为特定的dataset
     * 必须包含特定的 列
     */
    def to[T <: Product : TypeTag](): Dataset[T] = Basic.to[T](data)

    /** sugar
     */
    def groupBy(groupCols: Seq[String]) = {
      data.groupBy(groupCols.head: String, groupCols.tail: _*)
    }


    /**
     * 只选择相应列，而不转换
     */
    def get[T <: Product : TypeTag](): Dataset[_] = Basic.get[T](data)

    /**
     * 分组取样
     * 只取部分数据 ，已经 cache
     *
     * @param groups  分组取样对应的组
     * @param frac    比率
     * @param ceiling 最大组数
     */
    def groupSample(groups: Seq[String], frac: Double, ceiling: Int) = GroupSample.get(data, groups, frac, ceiling)

    /**
     * 将data frame 打印为指定的 case class( 部分复杂数据结构需要复制后重新修改)
     * 2021/4/26 新增
     */

    def toCaseClass(name: String) = {
      Test.printCaseClass(data, name)
    }


    def unite(newCol: String, cols: Seq[String], remove: Boolean) = Tidy.unite(data.toDF(), newCol, cols, remove)

    def unite(newCol: Seq[String], cols: Seq[Seq[String]], remove: Boolean) = Tidy.unite(data.toDF(), newCol, cols, remove)

    def uniteConcat(newCol: Seq[String], cols: Seq[Seq[String]], sep: Column) = Tidy.uniteConcat(data.toDF(), newCol, cols, true, sep)

    def uniteConcat(newCol: String, cols: Seq[String], sep: Column) = Tidy.uniteConcat(data.toDF(), newCol, cols, true, sep)

    /** 展开指定的 struct 列
     *
     * @param cols      需要  unstruct 的 列
     * @param remove    是否移除 struc 列
     * @param recursive 是否 递归，及 unstruct struct列里 的structype 的列
     */
    def separate(cols: Seq[String], remove: Boolean, recursive: Boolean): DataFrame = {
      Tidy.separate(data.toDF(), cols, remove, recursive)
    }

    //
    def separate(col: String, remove: Boolean, recursive: Boolean): DataFrame = {
      Tidy.separate(data.toDF(), Seq(col), remove, recursive)
    }


    /**
     * @param col    被切割的列
     * @param child  子列
     * @param remove 是否移除
     *               2021/5/12
     */
    def separate(col: String, child: Seq[String], remove: Boolean): DataFrame = {
      Tidy.separate(data.toDF(), col, child, remove)
    }

    def separate(col: String, newCol: String, child: Seq[String], remove: Boolean): DataFrame = {
      val df = Tidy.separate(data.toDF(), col, child, remove)
      Tidy.unite(df, newCol, child, true)
    }


    /** * 将同一类型的一些数据合并为同一列
     *
     * @param by    同一类型 的数据
     * @param key   分组的列
     * @param value 结果列
     */
    def gather(by: Seq[String], key: String, value: String): DataFrame = {
      Tidy.gather(data.toDF(), by, key, value)
    }

    /** 批量重命名的方法
     *
     * @param exists 已存在的列
     * @param news   新的列
     * @return 经过重命名的列，适用于对多列的重命名
     */
    def rename(exists: Seq[String], news: Seq[String]): Dataset[_] = {
      Basic.rename(data, exists, news)
    }

    /**
     * 所有列添加前缀
     */
    def rename(prefix: String): Dataset[_] = {
      Basic.rename(data, data.columns, prefix, "")
    }

    /**
     * 指定列添加前缀
     */
    def rename(cols: Seq[String], prefix: String): Dataset[_] = {
      Basic.rename(data, cols, prefix, "")
    }

    /**
     * 添加前后缀
     */
    def rename(cols: Seq[String], prefix: String, suffix: String): Dataset[_] = {
      Basic.rename(data, cols, prefix, suffix)
    }


    /**
     *
     * @param joinType join 类型
     * @param dataSeq  需要join 的其他df
     */

    @varargs
    def join(joinType: String, dataSeq: DataFrame*) = Basic.autoJoinByCommonCol(C(data, dataSeq), joinType)

  }


}
