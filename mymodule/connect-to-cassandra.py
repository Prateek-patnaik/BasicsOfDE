
from pyspark import SparkContext
sc = SparkContext("local", "employee-sample")
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


def load_and_get_table_df(keys_space_name, table_name):
    table_df = sqlContext.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keys_space_name) \
        .load()
    return table_df

emp = load_and_get_table_df("test", "employee_by_id")

emp.show()


from pyspark.sql.types import *
from pyspark.sql.functions import *


def flatten(df):
    # compute Complex Fields (Lists and Structs) in Schema
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    while len(complex_fields)!=0:
        col_name=list(complex_fields.keys())[0]
        print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(col_name+'_'+k) for k in [ n.name for n in  complex_fields[col_name]]]
            df=df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):
            df=df.withColumn(col_name,explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    return df



