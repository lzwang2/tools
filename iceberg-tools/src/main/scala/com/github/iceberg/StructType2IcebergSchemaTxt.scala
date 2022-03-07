package com.github.iceberg

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types._

/**
  * Created by lzwang2 on 2022/03/01.
  */
object StructType2IcebergSchemaTxt {

  var index = 1

  def dataTypeSchema(dataType: DataType, depth: Int): String = {

    var result = new StringBuilder
    val blank = "  " * depth
    val retractBlank = "  " * (depth - 1)

    if (dataType.isInstanceOf[StructType]) {

      result.append("Types.StructType.of(\n")
      val fields = dataType.asInstanceOf[StructType].fields
      val len = fields.length
      for (i: Int <- Range(0, len)) {
        result.append(blank)
        result.append("Types.NestedField.optional(")
        result.append(index)
        index += 1
        result.append(",\"")
        result.append(fields(i).name)
        result.append("\",")
        result.append(dataTypeSchema(fields(i).dataType, depth + 1))
        result.append(")")
        if (i < len - 1) {
          result.append(",")
        }
        result.append("\n")
      }
      result.append(retractBlank)
      result.append(")")

    } else if (dataType.isInstanceOf[BooleanType]) {
      result.append("Types.BooleanType.get")

    } else if (dataType.isInstanceOf[IntegerType]) {

      result.append("Types.IntegerType.get")

    } else if (dataType.isInstanceOf[LongType]) {

      result.append("Types.LongType.get")

    } else if (dataType.isInstanceOf[StringType]) {

      result.append("Types.StringType.get")

    } else if (dataType.isInstanceOf[DoubleType]) {

      result.append("Types.DoubleType.get")

    } else if (dataType.isInstanceOf[MapType]) {
      result.append("Types.MapType.ofOptional(")
      result.append(index)
      index += 1
      result.append(",")
      result.append(index)
      index += 1
      result.append(",")
      val mapType = dataType.asInstanceOf[MapType]
      result.append(dataTypeSchema(mapType.keyType, depth + 1))
      result.append(",")
      result.append(dataTypeSchema(mapType.valueType, depth + 1))
      result.append("\n")
      result.append(retractBlank)
      result.append(")")
    } else if (dataType.isInstanceOf[ArrayType]) {

      val isStructType = dataType.asInstanceOf[ArrayType].elementType.isInstanceOf[StructType]
      result.append("Types.ListType.ofOptional(")
      result.append(index)
      index += 1
      result.append(",")
      if (isStructType) {
        result.append("\n")
        result.append(blank)
      }
      result.append(dataTypeSchema(dataType.asInstanceOf[ArrayType].elementType, depth + 1))
      if (isStructType) {
        result.append("\n")
        result.append(retractBlank)
      }
      result.append(")")
    } else {
      println("unhandledType")
    }

    result
  }.toString()

}
