import sys
sys.path.append('/home/josemi/Documentos/parquets')
import os
from datetime import datetime, timedelta

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum, max, min , when, floor, trim, length, to_date, concat_ws, lpad, broadcast
from glue_custom import GLUE_CUSTOM
from glue_bancoestado import GLUE_BANCOESTADO
from data_connect import data_connect

MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT ='%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()

logger.setLevel(logging.INFO)

mysql_jdbc_driver_path = "mysql-connector-j-8.3.0.jar"

logger.info(f"Defino la ruta del jar driver {mysql_jdbc_driver_path}")


spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", mysql_jdbc_driver_path) \
    .config("spark.executor.extraClassPath", mysql_jdbc_driver_path) \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10g") \
    .getOrCreate()


def fecha(date):
    fecha_formateada = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    return fecha_formateada

def add_one_day(formatted_date):
    # Convert the formatted date string to a datetime object
    date_obj = datetime.strptime(formatted_date, '%Y-%m-%d')
    # Add one day using timedelta
    new_date_obj = date_obj + timedelta(days=1)
    # Convert the new datetime object back to a string in the same format
    new_date_str = new_date_obj.strftime('%Y-%m-%d')
    return new_date_str

def get_table_names(fecha_formateada, new_date_str):
    """
    Return a list of table names.
    """
    return {
        #"trx": "SELECT transactionid,encodedkey,parentaccountkey,balance,amount,details_encodedkey_oid FROM savingstransaction",
        #"cfv": "SELECT value, customfieldkey, parentkey, encodedkey FROM customfieldvalue",
        #"cta": "SELECT id,accountstate,encodedkey,producttypekey,blockedbalance,lockeddate FROM savingsaccount",
        #"prod": "SELECT id, name, encodedkey from savingsproduct",
        #"cli": "SELECT encodedkey from client"
        "min_trxid": f"SELECT MIN(trx.TRANSACTIONID) as id_transaction_min \
                        FROM  savingstransaction  AS trx \
                        INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY AND \
                            fechcontab.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' \
                        WHERE fechcontab.VALUE = {fecha_formateada}",
        "max_trxid": f"SELECT MAX(trx.TRANSACTIONID) AS id_transaction_max \
                        FROM  savingstransaction  AS trx \
                        INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY \
                        AND fechcontab.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' \
                        WHERE fechcontab.VALUE = {fecha_formateada}",
        "sum_amount": f"SELECT \
                        cta.ID AS numcta, \
                        SUM(trx.AMOUNT) AS amount, \
                        prod.ID AS producto \
FROM savingstransaction     AS trx \
INNER JOIN savingsaccount   AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
INNER JOIN savingsproduct   AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' \
INNER JOIN customfieldvalue AS adjuststate ON adjuststate.PARENTKEY = trx.ENCODEDKEY AND adjuststate.CUSTOMFIELDKEY = '8a818671756a87fc01756b28a2ee0b82' \
WHERE cfv.VALUE = {new_date_str} \
 AND adjuststate.VALUE = '0' \
 AND prod.ID IN ('CCV00002') \
 AND cta.ID NOT IN ( SELECT DISTINCT cta.ID \
                     FROM savingstransaction AS trx \
                     INNER JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
                     INNER JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
                     INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '8a81868f734d3e7001734f990b7122c5' \
                     WHERE prod.ID IN ('CCV00002') \
                     AND cfv.VALUE = {fecha_formateada}) \
GROUP BY cta.ID,prod.ID"
    }

    
    
def load_dataframes(queries, data_connect):
    """ Load tables into DataFrames """
 
    dataframes = {}
    
    for table_name, query in queries.items():
        try:
            df = spark.read.format("jdbc")\
                .option("url", "jdbc:mysql://" + data_connect['DB_HOST'] + ":3306/" + data_connect['DB_NAME'])\
                .option("query", query)\
                .option("user", data_connect['DB_USER'])\
                .option("password", data_connect['DB_PASSWORD'])\
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            dataframes[table_name] = df
            print(f"Loaded {table_name} into DataFrame.")
        except Exception as e:
            print(f"Error loading table {table_name}: {e}")
    
    return dataframes

def result_df(dataframes, fecha_formateada, GLUE_CUSTOM, GLUE_BANCOESTADO):
    """Function to filter and process dataframes to generate the final result dataframe."""
    
    #savingsaccount_df = dataframes['cta']
    #savingstransaction_df = dataframes['trx']
    #savingsproduct_df = dataframes['prod']
    #customfieldvalue_df = dataframes['cfv']
    #client_df = dataframes['cli']
    #transactionchannel_df = dataframes['transactionchannel']
    #transactiondetails_df = dataframes['transactiondetails']
    min_trxid = dataframes['min_trxid']
    max_trxid = dataframes['max_trxid']
    sum_amount_df = dataframes['sum_amount']
  
    #savingsaccount_df = savingsaccount_df.withColumnRenamed('id', 'ctaid')
    #savingsproduct_df = savingsproduct_df.withColumnRenamed('id', 'prodid')
    #transactionchannel_df = transactionchannel_df.withColumnRenamed('id', 'trxchid')

    #savingstransaction_df = savingstransaction_df.repartition(10)
    #savingsaccount_df = savingsaccount_df.repartition(10)
    # savingsproduct_df = savingsproduct_df.repartition(col('ENCODEDKEY'))
    #customfieldvalue_df = customfieldvalue_df.repartition(10)

    # product_ids = [prod_id.strip().strip("'") for prod_id in GLUE_BANCOESTADO['ALL_PRODUCTS'].split(',')]
    min_trxid.show()
    max_trxid.show()

    result = sum_amount_df


    return result

def main(date):

    fecha_formateada = fecha(date)
    logger.info(f"La fecha es: {fecha_formateada}")

    new_date_str = add_one_day(fecha_formateada)
    logger.info(f"La fecha del día siguiente es: {new_date_str}")

    queries = get_table_names(fecha_formateada, new_date_str)

    dataframes = load_dataframes(queries, data_connect)
    #dataframes['customfieldvalue'].select('value').show(20)

    result = result_df(dataframes, fecha_formateada, GLUE_CUSTOM, GLUE_BANCOESTADO)
    result.show(truncate=False)

    #print(result.show(20))
    #print(result.printSchema())
    #print(result.count())
    #result.write.mode('overwrite').option("header", True).option("delimiter", ";").csv("./output.txt")
    #result.coalesce(1).write.mode('overwrite').option("header", True).option("delimiter", ";").text("./output.txt")
    #result_001 = result.select(
    #    lpad(col("ctaid"), 14, '0').alias("ctaid_padded"),
    #    lpad(col('transactionid'), 14, '0').alias("transactionid_padded"),
    #    lpad(col('total_amount'), 14, '0').alias("total_amount_padded")
    #)

    #concatenated_df = result_001.select(
    #concat_ws(";", "ctaid_padded", "transactionid_padded", "total_amount_padded").alias("concatenated")
    #)
    #concatenated_df.show(truncate=False)
    #concatenated_df.coalesce(1).write.mode('overwrite').text("output_001.txt")


    #result_string = result.select(concat_ws(' ', col('transactionid').cast('string'), col('balance').cast('string'), col('amount').cast('string')))

    #result_string.show()  




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process date.")
    parser.add_argument("--date", type=str, help="Date parameter.")
    args = parser.parse_args()

    if args.date is None:
        print("Please provide a date parameter using the --date argument.")
    else:
        main(args.date)
