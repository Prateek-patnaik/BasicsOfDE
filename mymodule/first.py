from pyspark.sql import SparkSession
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

spark=SparkSession.builder.appName("sample").getOrCreate()

df_pyspark=spark.read.csv('sample.csv',header=True,inferSchema=True)

# df_pyspark.show()


# df_pyspark.write.option("header",True).csv('details1.csv')

df_pyspark=spark.read.csv('new_sample.csv',header=True,inferSchema=True)
df_pyspark.show()


print("connecting to Database")

hostname = 'localhost'
database = 'postgres'
username = 'postgres'
pwd = 'admin'
port_id = 5432

conn=None
cur=None
try:
    conn=psycopg2.connect(
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id)

    cur =conn.cursor()
    create_script = ''' CREATE TABLE IF NOT EXISTS details (
                                    SL_NO      varchar(100) PRIMARY KEY,
                                    ADDRESS    varchar(100),
                                    NAME    varchar(100),
                                    X1    varchar(100),
                                    X2    varchar(100),
                                    X3    varchar(100),
                                    X4    varchar(100),
                                    PLACE    varchar(100),
                                    DEVICE    varchar(100),
                                    VALUE    varchar(100))'''
    # cur.execute(create_script)
    #
    # print('script connected')
    # SQL_STATEMENT='''
    # COPY details(SL_NO,ADDRESS,NAME,X1,X2,X3,X4,PLACE,DEVICE,VALUE)
    #  FROM 'C:/Users/prateek.patnaik/IdeaProjects/demo_py/mymodule/new_sample.csv' WITH
    #     CSV
    #     HEADER
    #     DELIMITER As ',';
    # '''
    # cur.copy_expert(sql=SQL_STATEMENT,file=df_pyspark)
    # print('file copied to db')

    engine = create_engine(
        "postgresql+psycopg2://postgres:admin@localhost/postgres?client_encoding=utf8")
    pdf = pd.read_sql('select * from cities', engine)

    # Convert Pandas dataframe to spark DataFrame
    df = spark.createDataFrame(pdf)
    print(df.schema)
    df.show()


    conn.commit()

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()