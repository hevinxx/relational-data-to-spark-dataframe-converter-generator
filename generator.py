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

    if nullable == "t" or nullable == "true":
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
    field_names = []
    type_initials = []
    nullables = []
    schema = ""
    row = ""
    while True:
        field_name = raw_input("What is the field name? If you want to stop, just type 'stop'.  ")
        if field_name == "stop":
            break
        field_names.append(field_name)

    for item in field_names:
        type_initial = raw_input("What is the type of the {}? l/i/t/s    ".format(item))
        type_initials.append(type_initial)
    for item in field_names:
        nullable = raw_input("is {} nullable? Default is false. ".format(item))
        nullables.append(nullable)
    for i, item in enumerate(field_names):
        schema += "    " + buildSchema(item, type_initials[i], nullables[i]) + ",\n"
        row += "        " + buildRow(i, type_initials[i]) + ",\n"
    schema = schema[:-2]
    row = row[:-2]
    result = """%spark
val {}Url = s"hdfs://0.0.0.0:9000/user/root/{}/part-m-0000*"
val {}Rdd = sc.textFile({}Url, 4).map(line => line.split("\\u0001").to[List])

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
