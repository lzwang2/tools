package com.github.iceberg

import org.apache.spark.sql.types.{StructField, StructType, _}

/**
  * Created by lzwang on 2022/03/01.
  */
object TestStructType {

  val STRUCT_SCHEMA = StructType(
    Seq (
      StructField("string_field", StringType, true),
      StructField("integer_field", IntegerType, true)
    )
  )

  val TEST_SCHEMA = StructType(
    Seq (
      StructField("string_field", StringType, true),
      StructField("integer_field", IntegerType, true),
      StructField("double_field", DoubleType, true),
      StructField("boolean_field", BooleanType, true),
      StructField("struct_field", IntegerType, true),
      StructField("simple_array_field", ArrayType(StringType), true),
      StructField("simple_map_field", MapType(StringType, IntegerType), true),
      StructField("complex_array_field", ArrayType(STRUCT_SCHEMA), true),
      StructField("complex_map_field", MapType(StringType, STRUCT_SCHEMA), true)
    )
  )
}
