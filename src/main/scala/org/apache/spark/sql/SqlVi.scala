package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.StringUtils.StringConcat
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

/**
 * 为了访问部分 sql 包可见的方法
 */
object SqlVi {


  def buildFormattedString(fields: StructType, prefix: String, builder: StringConcat): Unit = {

    fields.foreach(field => {
      field.dataType match {
        case _: ArrayType =>
          builder.append(s"$prefix-- ${field.name}: Seq[_],\n")
        case _: StructType =>
          builder.append(s"$prefix-- ${field.name}: _,\n")
        case _: MapType =>
          builder.append(s"$prefix-- ${field.name}: Map[_,_],\n")
        case _ => field.buildFormattedString(prefix, builder, Int.MaxValue)
      }

    })

  }
}