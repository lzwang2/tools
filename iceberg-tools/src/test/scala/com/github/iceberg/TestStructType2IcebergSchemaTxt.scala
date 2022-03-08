package com.github.iceberg

object TestStructType2IcebergSchemaTxt {

  def main(args: Array[String]): Unit = {
    println(StructType2IcebergSchemaTxt.dataTypeSchema(TestStructType.TEST_SCHEMA, 0))
    //The following should be output
    //final iceberg schema after adjustment, see TestIcebergSchema
    /**
      Types.StructType.of(
      Types.NestedField.optional(1,"string_field",Types.StringType.get),
      Types.NestedField.optional(2,"integer_field",Types.IntegerType.get),
      Types.NestedField.optional(3,"double_field",Types.DoubleType.get),
      Types.NestedField.optional(4,"boolean_field",Types.BooleanType.get),
      Types.NestedField.optional(5,"struct_field",Types.IntegerType.get),
      Types.NestedField.optional(6,"simple_array_field",Types.ListType.ofOptional(7,Types.StringType.get)),
      Types.NestedField.optional(8,"simple_map_field",Types.MapType.ofOptional(9,10,Types.StringType.get,Types.IntegerType.get
      )),
      Types.NestedField.optional(11,"complex_array_field",Types.ListType.ofOptional(12,
        Types.StructType.of(
          Types.NestedField.optional(13,"string_field",Types.StringType.get),
          Types.NestedField.optional(14,"integer_field",Types.IntegerType.get)
        )
      )),
      Types.NestedField.optional(15,"complex_map_field",Types.MapType.ofOptional(16,17,Types.StringType.get,Types.StructType.of(
          Types.NestedField.optional(18,"string_field",Types.StringType.get),
          Types.NestedField.optional(19,"integer_field",Types.IntegerType.get)
        )
      ))
      ) */
  }

}
