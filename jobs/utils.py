def read_data(spark,input_path):
    df = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(input_path)
    return  df

def write_data(df,output_path):
    df.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(output_path)
