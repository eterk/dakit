package org.cars.dakit.sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}


object GroupSample {
  def get(data: Dataset[_], groups: Seq[String], frac: Double, ceiling: Int): Dataset[_] = {
    new GroupSample(data, groups).setFrac(frac).setCeiling(ceiling).refresh().transform()
  }
}



class GroupSample private(data: Dataset[_], groups: Seq[String]) {
  private var ceiling = 3
  private var frac = 0.01
  lazy val groupData = data.groupBy(groups.map(col(_)): _*).count()
  private var sampleGroup: DataFrame = sample

  def setFrac(f: Double): this.type = {
    this.frac = f
    this
  }

  def getFrac(): Double = {
    this.frac
  }


  def setCeiling(c: Int): this.type = {
    this.ceiling = c
    this
  }

  def getCeiling(): Int = {
    this.ceiling
  }

  private def sample: DataFrame = {
    val a1 = groupData.sample(frac).limit(ceiling)
    a1.persist()
    a1
  }

  def refresh(): this.type = {
    this.sampleGroup = sample
    this
  }

  def getSampleGroup(): DataFrame = {
    sampleGroup
  }

  def transform(): DataFrame = {
    data.join(sampleGroup, groups, "left_semi")
  }
}