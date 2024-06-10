import sys
sys.path.append('/home/josemi/Documentos/parquets')
from datetime import datetime, timedelta
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import lit, col, sum, max, min , when, floor, trim, length, to_date, date_add, concat_ws, lpad
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

accountant_date_str = '2024-02-01'

# Convert the string to a datetime object
accountant_date = datetime.strptime(accountant_date_str, "%Y-%m-%d")

# Add 1 day to the date using timedelta
next_accountant_date = accountant_date + timedelta(days=1)

accountant_date_formatted = accountant_date.strftime("%Y-%m-%d")
next_accountant_date_formatted = next_accountant_date.strftime("%Y-%m-%d")

print(f"El día hábil siguiente a la fecha '{accountant_date_formatted}' es {next_accountant_date_formatted}")
logger.info(f" Fecha contable: {accountant_date_formatted}. Fecha hábil siguiente a fecha contable: {next_accountant_date_formatted}")

max_trx_id = '2680342'

"""
query_maxtrxid = " \
SELECT \
        MAX(trx.TRANSACTIONID) AS id_transaction_max \
    FROM \
        savingstransaction trx \
        LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
        LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
        INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY AND fechcontab.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechcontab'] + "' \
        INNER JOIN customfieldvalue AS codblq ON codblq.PARENTKEY = cta.ENCODEDKEY AND codblq.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_codblq'] + "' \
    WHERE \
        fechcontab.VALUE IN (f"'{accountant_date}'") \
        AND prod.ID IN (" + GLUE_BANCOESTADO['ALL_PRODUCTS'] + ") \
        AND NOT (codblq.VALUE = '99' AND cta.ACCOUNTSTATE = 'LOCKED') \
"
"""
transaction_sum = "SELECT \
    cta.ID AS numcta, \
    SUM(trx.AMOUNT) AS amount, \
    prod.ID AS producto \
FROM \
    savingstransaction AS trx \
    LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
    LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
    LEFT JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechcontab'] + "' \
WHERE \
    ( \
        cfv.VALUE = '" + next_accountant_date_formatted + "' AND trx.TRANSACTIONID <= '" + max_trx_id + "' \
    ) \
    AND prod.ID IN (" + GLUE_BANCOESTADO['ALL_PRODUCTS'] + ") \
    AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
    AND cta.ID NOT IN ( \
        SELECT \
            cta.ID \
        FROM \
            savingstransaction AS trx \
            INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechcontab'] + "' \
            LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
            LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
        WHERE \
            prod.ID IN (" + GLUE_BANCOESTADO['ALL_PRODUCTS'] + ") \
            AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
            AND cfv.VALUE = '" + str(accountant_date_formatted) + "' \
        ) \
    GROUP BY cta.ID"

cw_improvement = " \
    SELECT \
    cfvc.VALUE AS rut, \
    cfvtipocli.VALUE AS tipocliente, \
    trx.BALANCE AS saldo, \
    cta.ID AS numcta, \
    '00000' AS numtrx, \
    cfvctabech.VALUE AS numctabech, \
    cta.ACCOUNTSTATE AS estado, \
    cfvfecrut.VALUE AS fechaaperut, \
    cta.ACTIVATIONDATE AS fechaactiv, \
    cta.CLOSEDDATE AS fechacierre, \
    cta.BLOCKEDBALANCE AS saldo_bloqueado, \
    trx.TRANSACTIONID AS transaccion, \
    cfvofi.VALUE AS oficina, \
    ret.retencion_1dia,  \
    ret.retencion_2dias,  \
    ret.retencion_ndias,  \
    ruttutor.VALUE AS rut_tutor,  \
    cfv.VALUE AS fecha_contable_real,  \
    prod.ID AS producto  \
FROM  \
    savingstransaction trx \
    INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
    INNER JOIN savingsproduct prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY  \
    INNER JOIN client cli ON cli.ENCODEDKEY = cta.ACCOUNTHOLDERKEY  \
    LEFT JOIN customfieldvalue cfvc ON cfvc.PARENTKEY = cli.ENCODEDKEY AND cfvc.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_rut'] + "' \
    LEFT JOIN customfieldvalue cfvtipocli ON cfvtipocli.PARENTKEY = cta.ENCODEDKEY AND cfvtipocli.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_tipocliente'] + "' \
    LEFT JOIN customfieldvalue cfvctabech ON cfvctabech.PARENTKEY = cta.ENCODEDKEY AND cfvctabech.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_numctabech'] + "' \
    LEFT JOIN customfieldvalue cfvfecrut ON cfvfecrut.PARENTKEY = cta.ENCODEDKEY AND cfvfecrut.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechaperut'] + "' \
    LEFT JOIN customfieldvalue cfvofi ON cfvofi.PARENTKEY = cta.ENCODEDKEY AND cfvofi.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_ofiprod'] + "' \
    LEFT JOIN customfieldvalue ruttutor ON ruttutor.PARENTKEY = cta.ENCODEDKEY AND ruttutor.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_ruttutor'] + "' \
    LEFT JOIN customfieldvalue cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_fechcontab'] + "' \
    INNER JOIN (\
        SELECT \
            cta.ID AS nro_cta, \
            MAX(trx.TRANSACTIONID) AS MaxTransactionID \
        FROM  \
            savingstransaction trx \
            INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
            WHERE trx.TRANSACTIONID <= (SELECT MAX(trx2.TRANSACTIONID)  \
                                         FROM savingstransaction trx2  \
                                         INNER JOIN customfieldvalue cfv2 ON cfv2.PARENTKEY = trx2.ENCODEDKEY  \
                                         WHERE cfv2.VALUE = '" + accountant_date_formatted + "') \
        GROUP BY cta.ID \
    ) AS maxtrx ON maxtrx.nro_cta = cta.ID AND trx.TRANSACTIONID = maxtrx.MaxTransactionID \
    LEFT JOIN ( \
        SELECT  \
            cta.ID AS nro_cta,  \
            SUM(CASE WHEN cfvtr.VALUE = '1' THEN trx.AMOUNT ELSE 0 END) AS retencion_1dia,  \
            SUM(CASE WHEN cfvtr.VALUE IN ('2', '3') THEN trx.AMOUNT ELSE 0 END) AS retencion_2dias,  \
            SUM(CASE WHEN cfvtr.VALUE = 'N' THEN trx.AMOUNT ELSE 0 END) AS retencion_ndias \
        FROM  \
            savingstransaction trx \
            INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
            LEFT JOIN customfieldvalue cfvtr ON cfvtr.PARENTKEY = trx.ENCODEDKEY  \
        WHERE  \
            cfvtr.CUSTOMFIELDKEY = '" + GLUE_CUSTOM['_tiporetencion'] + "' AND \
            trx.TRANSACTIONID <= (SELECT MAX(trx2.TRANSACTIONID)  \
                                  FROM savingstransaction trx2  \
                                  INNER JOIN customfieldvalue cfv2 ON cfv2.PARENTKEY = trx2.ENCODEDKEY  \
                                  WHERE cfv2.VALUE = '" + accountant_date_formatted + "') \
        GROUP BY cta.ID \
    ) AS ret ON ret.nro_cta = cta.ID \
WHERE  \
    prod.ID IN (" + GLUE_BANCOESTADO['ALL_PRODUCTS'] + ")  \
    AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
"

customfieldvalue = "SELECT VALUE, CUSTOMFIELDKEY, ENCODEDKEY, PARENTKEY FROM customfieldvalue"

logger.info("1. Empiezo a generar df...")
df = spark.read.format("jdbc")\
    .option("url", "jdbc:mysql://" + data_connect['DB_HOST'] + ":3306/" + data_connect['DB_NAME'])\
    .option("query", cw_improvement)\
    .option("user", data_connect['DB_USER'])\
    .option("password", data_connect['DB_PASSWORD'])\
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

df.repartition(100)
df.persist(StorageLevel.MEMORY_ONLY)

print(df.select('numcta', 'fechaaperut', 'saldo_bloqueado', 'transaccion').show(5))

print(f"Las líneas contadas son: {df.count()}.")

#logger.info("2. Finalizo generación de df...")

#logger.info("3. Inicio rellenado de ceros a la izquierda...")
#zflld_df = df.select(
#    lpad('value', 35, '0').alias('value_filled'),
#    lpad('customfieldkey', 35, '0').alias('customfieldkey_filled'),
#    lpad('encodedkey', 35, '0').alias('encodedkey_filled'),
#    lpad('parentkey', 35, '0').alias('parentkey_filled')
#    )
#logger.info("4. Finalizado rellenado de ceros a la izquierda...")

#logger.info("5. Iniciando separación de columnas con carácter '+'...")
#cctntd_df = zflld_df.select(
#    concat_ws("+", 'value_filled', 'customfieldkey_filled', 'encodedkey_filled', 'parentkey_filled')
#        )
#logger.info("6. Finalizado proceso separación de columnas con carácter '+'...")

#logger.info("7. Iniciando escritura en archivo único...")
#cctntd_df.coalesce(1).write.mode('overwrite').text("archivo_unico_001.txt") 22:22:35 - 22:23:14
#cctntd_df.write.format('text').mode('overwrite').save("archivo_unico_001.txt")
#logger.info("8. Finaliza proceso escritura en archivo único...")
#logger.info("9. Contando filas del dataframe...")
#logger.info(f"10. Se escribieron {df.count()} filas en archivo único...")
#logger.info("11. Finaliza el conteo del total de filas escritas")



# ['rut', 'tipocliente', 'saldo', 'numcta', 'numtrx', 'numctabech', 'estado', 'fechaaperut', 'fechaactiv', 'fechacierre', 'saldo_bloqueado', 'transaccion', 'oficina', 'retencion_1dia', 'retencion_2dias', 'retencion_ndias', 'rut_tutor', 'fecha_contable_real', 'producto']


#df.select('rut','numcta').filter(~(col('rut').isNull())).groupby('rut','numcta').agg(lit('new_col')).show()

