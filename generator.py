def buildSchema(field_name, type_initial, nullable):
    type_name = ""
    if type_initial == "l":
        type_name = "LongType"
    elif type_initial == "i":
        type_name = "IntegerType"
    elif type_initial == "t":
        type_name = "TimestampType"
    elif type_initial == "s":
        type_name = "StringType"

    if nullable == "t":
        nullable = "true"
    else:
        nullable = "false"

    result = """StructField("{}", {}, {})""".format(field_name, type_name, nullable)
    return result

def buildRow(index, type_initial):
    type_name = ""
    if type_initial == "l":
        type_name = "Long"
    elif type_initial == "i":
        type_name = "Int"
    elif type_initial == "t":
        type_name = "Timestamp"
    elif type_initial == "s":
        return """line({})""".format(index)

    result = """line({}).to{}Safe.getOrElse(null)""".format(index, type_name)
    return result

if __name__ == "__main__":
    table_name = raw_input("What is the table name? ")
    schema = ""
    row = ""
    index = 0
    while True:
        field_name = raw_input("What is the field name? If you want to stop, just type 'stop'.  ")
        if field_name == "stop":
            schema = schema[:-2]
            row = row[:-2]
            break
        type_initial = raw_input("What is the type of the field? l/i/t/s    ")
        nullable = raw_input("Is it nullable? Default is false. ")
        schema += "    " + buildSchema(field_name, type_initial, nullable) + ",\n"
        row += "        " + buildRow(index, type_initial) + ",\n"
        index += 1
    result = """%spark
val {}Url = s"hdfs://0.0.0.0:9000/user/root/{}/part-m-0000*"
val {}Rdd = sc.textFile({}Url, 4).map(line => line.split("\\001").to[List])

val {}Schema = StructType(Seq(
{}
))

def {}Row(line: List[String]): Row = {{
    Row(
{}
    )
}}

val {} = spark.createDataFrame({}Rdd.map({}Row), {}Schema)
{}.write.mode("overwrite").saveAsTable("{}")
""".format(table_name, table_name, table_name, table_name, table_name, schema, table_name, row, table_name, table_name, table_name, table_name, table_name, table_name)
    print(result)
