import sys
sys.path.append('/home/josemi/Documentos/parquets')
import os
import ast
import argparse
import logging
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DecimalType
from pyspark.sql import Window
from glue_custom import GLUE_CUSTOM
from glue_bancoestado import GLUE_BANCOESTADO
from data_connect import data_connect
from pyspark.sql import Row

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
    .config("spark.local.dir", "/home/josemi") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "4") \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumColumns", 100)
spark.conf.set("spark.sql.repl.eagerEval.truncate", 100)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 100)
spark.conf.set("spark.sql.debug.maxToStringFields", 2000)

#spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive", True)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "250m")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

spark.conf.set("spark.sql.join.preferSortMergeJoin", True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 1)

def fecha(date):
    fecha_formateada = f"{date[:4]}-{date[4:6]}-{date[6:]}"
    return fecha_formateada

def first_month_day(fecha_formateada):
        formatDate = datetime.datetime.strptime(fecha_formateada, "%Y-%m-%d")
        firstDate = formatDate.replace(day=1)
        firstDateStr = firstDate.strftime("%Y-%m-%d")
        return firstDateStr


def next_acc_day(fecha_formateada):
        formatDate = datetime.datetime.strptime(fecha_formateada, "%Y-%m-%d")
        getNextDay = formatDate + datetime.timedelta(days=1)
        nextDay = getNextDay.strftime("%Y-%m-%d")
        logger.info(f"El día siguiente a {fecha_formateada} es: {nextDay}")
        return nextDay


def get_table_names(GLUE_CUSTOM):

    return {
        "trx": "SELECT transactionid, encodedkey, parentaccountkey, balance, amount, details_encodedkey_oid FROM savingstransaction",
        "cta": "SELECT id, accountstate, encodedkey, producttypekey, blockedbalance, lockeddate, closeddate, accountholderkey, activationdate FROM savingsaccount",
        "prod": f"SELECT id, encodedkey FROM savingsproduct",
        "cli": "SELECT encodedkey from client",
        "trxdet": f"SELECT encodedkey, transactionchannelkey FROM transactiondetails",
        "trxch": "SELECT id, encodedkey FROM transactionchannel",
        "cfv": f"SELECT value, parentkey, customfieldkey FROM customfieldvalue",
        "hld": f"SELECT ISANNUALYRECURRING, MONTHOFYEAR, DAYOFMONTH, YEAR, NAME FROM holiday"
    }
    
def load_dataframes(queries, data_connect):
    """Cargar resultados de las queries en dataframes"""
 
    dataframes = {}
    
    for table_name, query in queries.items():
        try:
            df = spark.read.format("jdbc") \
                .option("url", "jdbc:mysql://" + data_connect['DB_HOST'] + ":3306/" + data_connect['DB_NAME']) \
                .option("query", query) \
                .option("user", data_connect['DB_USER']) \
                .option("password", data_connect['DB_PASSWORD']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            dataframes[table_name] = df
            #logger.info(f"Tabla {table_name} cargada a DataFrame.")
        except Exception as e:
            logger.info(f"Error al cargar tabla {table_name}: {e}")
    
    return dataframes
    
def result_df(dataframes, fecha_formateada, nextDay, GLUE_BANCOESTADO):
    """Función para filtrar y procesar dataframes para generar resultado final."""
    
    """Llamo dataframes y defino nombres"""
    # Repartition and cache dataframes
    num_partitions = 100  # Adjust the number of partitions as needed

    cta_df = dataframes['cta'].repartition(num_partitions).cache()
    trx_df = dataframes['trx'].repartition(num_partitions).cache()
    prod_df = dataframes['prod'].cache()
    cfv_df = dataframes['cfv'].repartition(num_partitions).cache()
    cli_df = dataframes['cli'].repartition(num_partitions).cache()
    #trxch_df = dataframes['trxch'].repartition(num_partitions).cache()
    #trxdet_df = dataframes['trxdet'].repartition(num_partitions).cache()
    hld_df = dataframes['hld'].repartition(num_partitions).cache()


    logger.info(f"Dataframes generados exitosamente.")

    logger.info(f"Creación de vistas.")
    savingsaccount = cta_df.createOrReplaceTempView('savingsaccount')
    savingstransaction = trx_df.createOrReplaceTempView('savingstransaction')
    savingsproduct = prod_df.createOrReplaceTempView('savingsproduct')
    client = cli_df.createOrReplaceTempView('client')
    customfieldvalue = cfv_df.createOrReplaceTempView('customfieldvalue')
    #transactiondetails = trxdet_df.createOrReplaceTempView('transactiondetails')
    #transactionchannel = trxch_df.createOrReplaceTempView('transactionchannel')
    holiday = hld_df.createOrReplaceTempView('holiday')
    logger.info(f"Fin creación de vistas.")

    """
    query1 = (f" \
                        SELECT CASE \
            WHEN (ISANNUALYRECURRING = 1) THEN DATE_FORMAT(CONCAT(DATE_FORMAT(CURDATE(), '%Y'), '-', MONTHOFYEAR, '-', DAYOFMONTH), '%Y-%m-%d') \
            ELSE DATE_FORMAT(CONCAT(YEAR, '-', MONTHOFYEAR, '-', DAYOFMONTH), '%Y-%m-%d') \
            END AS \
                holidays, \
                NAME AS NAME, \
                ISANNUALYRECURRING AS IS_RECURRING, \
                DATE_FORMAT(CONCAT(YEAR, '-', MONTHOFYEAR, '-', DAYOFMONTH), '%Y-%m-%d') AS DATE_PLATFORM \
            FROM holiday \
        ")
    festivos = spark.sql(query1)
    """
    festivos = spark.table("holiday").select(
        F.when(F.col("ISANNUALYRECURRING") == 1,
            F.date_format(F.concat(F.date_format(F.lit("2024-01-01"), "yyyy"), F.lit('-'), F.col("MONTHOFYEAR"), F.lit('-'), F.col("DAYOFMONTH")), "yyyy-MM-dd")
            ).otherwise(
            F.date_format(F.concat(F.col("YEAR"), F.lit('-'), F.col("MONTHOFYEAR"), F.lit('-'), F.col("DAYOFMONTH")), "yyyy-MM-dd")
        ).alias("holidays"),
        F.col("NAME").alias("NAME"),
        F.col("ISANNUALYRECURRING").alias("IS_RECURRING"),
        F.date_format(F.concat(F.col("YEAR"), F.lit('-'), F.col("MONTHOFYEAR"), F.lit('-'), F.col("DAYOFMONTH")), "yyyy-MM-dd").alias("DATE_PLATFORM")
    )

    # Log and display the results
    #festivos.printSchema()
    #festivos.show()


    query2 = (f" \
                             SELECT \
        MAX(trx.TRANSACTIONID) AS id_transaction_max \
    FROM \
        savingstransaction trx \
        LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
        LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
        INNER JOIN customfieldvalue fechcontab ON fechcontab.PARENTKEY = trx.ENCODEDKEY AND fechcontab.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_fechcontab']}' \
    WHERE \
        fechcontab.VALUE = '{fecha_formateada}' \
        AND prod.ID IN ('{GLUE_BANCOESTADO['ALL_PRODUCTS']}') \
        ")
    get_maxtrxid = spark.sql(query2)
    get_maxtrxid.show()
    max_trx_id = get_maxtrxid.select('id_transaction_max').collect()[0][0]
    
    query3 = (f" \
                SELECT \
        cta.ID AS numcta, \
        SUM(trx.AMOUNT) AS amount, \
        prod.ID AS producto \
    FROM \
        savingstransaction AS trx \
        LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
        LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
        LEFT JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_fechcontab']}' \
    WHERE \
        ( \
            cfv.VALUE = '{nextDay}' AND trx.TRANSACTIONID <= '{max_trx_id}' \
        ) \
        AND prod.ID IN ('{GLUE_BANCOESTADO['ALL_PRODUCTS']}') \
        AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
        AND cta.ID NOT IN ( \
            SELECT \
                cta.ID \
            FROM \
                savingstransaction AS trx \
                INNER JOIN customfieldvalue AS cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_fechcontab']}' \
                LEFT JOIN savingsaccount AS cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY \
                LEFT JOIN savingsproduct AS prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY \
            WHERE \
                prod.ID IN ('{GLUE_BANCOESTADO['ALL_PRODUCTS']}') \
                AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
                AND cfv.VALUE = '{fecha_formateada}' \
            ) \
        GROUP BY cta.ID, prod.ID \
              ")
    #logger.info(query3)
    trx_day_after = spark.sql(query3)
    #trx_day_after.show()
    #trx_day_after.printSchema()
    trx_day_after_nonzero = trx_day_after.filter(F.col('amount') != 0)
    trx_day_after_nonzero.cache()
    #trx_day_after_nonzero.show()


    query4 = (f" \
                  SELECT \
        cfvc.VALUE AS rut, \
        cfvtipocli.VALUE AS tipocliente, \
        trx.BALANCE AS saldo, \
        cta.ID AS numcta2, \
        '00000' AS numtrx, \
        cfvctabech.VALUE AS numctabech, \
        cta.ACCOUNTSTATE AS estado, \
        cfvfecrut.VALUE AS fechaaperut, \
        cta.ACTIVATIONDATE AS fechaactiv, \
        cta.CLOSEDDATE AS fechacierre, \
        cta.BLOCKEDBALANCE AS saldo_bloqueado, \
        trx.TRANSACTIONID AS id_trx, \
        cfvofi.VALUE AS oficina, \
        ret.retencion_1dia,  \
        ret.retencion_2dias,  \
        ret.retencion_ndias,  \
        ruttutor.VALUE AS rut_tutor,  \
        cfv.VALUE AS fecha_contable_real,  \
        prod.ID AS producto,  \
        codblq.VALUE AS causal_blq\
    FROM  \
        savingstransaction trx \
        INNER JOIN savingsaccount cta ON cta.ENCODEDKEY = trx.PARENTACCOUNTKEY  \
        INNER JOIN savingsproduct prod ON prod.ENCODEDKEY = cta.PRODUCTTYPEKEY  \
        INNER JOIN client cli ON cli.ENCODEDKEY = cta.ACCOUNTHOLDERKEY  \
        LEFT JOIN customfieldvalue cfvc ON cfvc.PARENTKEY = cli.ENCODEDKEY AND cfvc.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_rut']}' \
        LEFT JOIN customfieldvalue cfvtipocli ON cfvtipocli.PARENTKEY = cta.ENCODEDKEY AND cfvtipocli.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_tipocliente']}' \
        LEFT JOIN customfieldvalue cfvctabech ON cfvctabech.PARENTKEY = cta.ENCODEDKEY AND cfvctabech.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_numctabech']}' \
        LEFT JOIN customfieldvalue cfvfecrut ON cfvfecrut.PARENTKEY = cta.ENCODEDKEY AND cfvfecrut.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_fechaperut']}' \
        LEFT JOIN customfieldvalue cfvofi ON cfvofi.PARENTKEY = cta.ENCODEDKEY AND cfvofi.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_ofiprod']}' \
        LEFT JOIN customfieldvalue ruttutor ON ruttutor.PARENTKEY = cta.ENCODEDKEY AND ruttutor.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_ruttutor']}' \
        LEFT JOIN customfieldvalue cfv ON cfv.PARENTKEY = trx.ENCODEDKEY AND cfv.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_fechcontab']}' \
        LEFT JOIN customfieldvalue codblq ON codblq.PARENTKEY = cta.ENCODEDKEY AND codblq.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_codblq']}' \
        LEFT JOIN customfieldvalue adjuststate ON adjuststate.PARENTKEY = trx.ENCODEDKEY AND adjuststate.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_adjust_state']}' \
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
                                             WHERE cfv2.VALUE = '{fecha_formateada}') \
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
                cfvtr.CUSTOMFIELDKEY = '{GLUE_CUSTOM['_tiporetencion']}' AND \
                trx.TRANSACTIONID <= (SELECT MAX(trx2.TRANSACTIONID)  \
                                      FROM savingstransaction trx2  \
                                      INNER JOIN customfieldvalue cfv2 ON cfv2.PARENTKEY = trx2.ENCODEDKEY  \
                                      WHERE cfv2.VALUE = '{fecha_formateada}') \
            GROUP BY cta.ID \
        ) AS ret ON ret.nro_cta = cta.ID \
    WHERE \
        CASE \
            WHEN LEFT(cta.lockeddate, 10) = '{fecha_formateada}' \
                THEN \
                    prod.ID IN ('{GLUE_BANCOESTADO['ALL_PRODUCTS']}')  \
                    AND cta.ACCOUNTSTATE IN ('ACTIVE', 'LOCKED') \
                    AND adjuststate.VALUE = '0' \
                ELSE cta.accountstate IN ('ACTIVE', 'LOCKED') AND NOT (codblq.value = '99' AND cta.accountstate = 'LOCKED') \
        END \
                  ")
    #logger.info(f"{query4}")

    main_query = spark.sql(query4)
    main_query.cache()

    result = main_query.join(F.broadcast(trx_day_after_nonzero), main_query['numcta2'] == trx_day_after_nonzero['numcta']) \
                       .select(
                            trx_day_after_nonzero['numcta'],
                            trx_day_after_nonzero['amount'],
                            main_query['saldo'],
                            main_query['numcta2']) \
                        .withColumn('resta', F.lpad((F.col('saldo') - F.col('amount')).cast(DecimalType(15, 3)).cast(StringType()), 15, '0'))


    return result
"""
def logicaNegocio(result):

    final_result = result \
        .withColumn("ctry", F.lit("CL ")) \
        .withColumn("fec_cont", F.date_format(F.lit("2024-01-01"), "yyyyMMdd")) \
        .withColumn("intf_dt", F.date_format(F.lit("2024-01-01"), "yyyyMMdd")) \
        .withColumn("src_id", F.rpad(F.lit("OPB3"), 14, " ")) \
        .withColumn("cem", F.lit("012")) \
        .withColumn("of_cta", F.when(F.col("cfvofprod_VALUE").isNull(), F.rpad(F.lit(" "), 10, " ")).otherwise(F.rpad(F.col("cfvofprod_VALUE"), 10, " "))) \
        .withColumn("state", F.when(F.col("cta_ACCOUNTSTATE") == "ACTIVE", F.rpad(F.lit("A"), 10, " "))
                                    .when(F.col("cta_ACCOUNTSTATE") == "LOCKED", F.rpad(F.lit("A"), 10, " "))
                                    .when(F.col("cta_ACCOUNTSTATE") == "CLOSED", F.rpad(F.lit("C"), 10, " "))
                                    .otherwise(F.rpad(F.lit(" "), 10, " "))) \
        .withColumn("dinq_sta", F.lit("1")) \
        .withColumn("prod", F.rpad(F.concat(F.lit("CBC "), F.col("prod_ID")), 16, " ")) \
        .withColumn("open_dt", F.when(F.col('cfvofprod_VALUE').isNull(), F.rpad(F.lit(" "), 8, " "))) \
        .withColumn("lst_accr_dt", F.when(F.col("cta_CLOSEDDATE").isNull(), F.rpad(F.lit(" "), 8, " "))
                                    .otherwise(F.rpad(F.date_format(F.col("cta_CLOSEDDATE"), "yyyyMMdd"), 8, " "))) \
        .withColumn("resp_rut", F.when(F.col("cfvrut_VALUE").isNull(), F.rpad(F.lit(" "), 25, " "))
                                    .otherwise(F.rpad(F.lpad(F.col("cfvrut_VALUE"), 11, "0"), 25, " "))) \
        .withColumn("cc", F.rpad(F.col("cfvofprod_VALUE"), 10, " ")) \
        .withColumn("nro_cta", F.when(F.col("cta_ID").isNull(), F.rpad(F.lit(" "), 30, " "))
                                .otherwise(F.rpad(F.col("cta_ID"), 30, " "))) \
        .withColumn("fec_ini", F.when(F.col("cfvfchap_VALUE").isNull(), F.rpad(F.lit(" "), 8, " "))
                                    .otherwise(F.rpad(F.date_format(F.col("cfvfchap_VALUE"), "yyyyMMdd"), 8, " "))) \
        .withColumn("closed_date", F.when(F.col("cta_CLOSEDDATE").isNull(), F.rpad(F.lit(" "), 8, " "))
                                    .otherwise(F.rpad(F.date_format(F.col("cta_CLOSEDDATE"), "yyyyMMdd"), 8, " "))) \
        .withColumn("fec_ren", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("int_antc", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("cod_moned", F.lit("999 ")) \
        .withColumn("sig", F.when(F.col("trx_BALANCE") > 0, F.lit("+")).otherwise(F.lit("-"))) \
        .withColumn("amount", F.when(F.col("trx_BALANCE").isNull(), F.lpad(F.lit("0"), 20, "0"))
                                .otherwise(F.lpad(F.round(F.col("trx_BALANCE"), 4).cast("string"), 20, "0"))) \
        .withColumn("sig_amount", F.when(F.col("trx_BALANCE") > 0, F.lit("+")).otherwise(F.lit("-"))) \
        .withColumn("cap_amount", F.when(F.col("trx_BALANCE").isNull(), F.lpad(F.lit("0"), 20, "0"))
                                .otherwise(F.lpad(F.round(F.col("trx_BALANCE"), 2).cast("string"), 20, "0"))) \
        .withColumn("lin_cred_amount", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lcy_reaj_amnt_sig", F.lit("+")) \
        .withColumn("lcy_reaj_amnt", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("ocy_int_amnt_sig", F.lit("+")) \
        .withColumn("ocy_int_amnt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lcy_int_amnt_sig", F.lit("+")) \
        .withColumn("lcy_int_amnt", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("fix_flting_ind", F.lit("  ")) \
        .withColumn("int_rt_cod", F.lit("    ")) \
        .withColumn("int_rt", F.concat(F.lit("+"), F.lpad(F.lit("0.00000000"), 16, "0"))) \
        .withColumn("pnlt_rt", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("rt_meth", F.lit(" ")) \
        .withColumn("pool_rt", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("pool_rt_cod", F.lpad(F.lit("0"), 5, " ")) \
        .withColumn("pnlt_rt_cod", F.lit("    ")) \
        .withColumn("int_rt_sprd", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("pool_rt_sprd", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("pnlt_rt_sprd", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("aset_liad_ind", F.lit("P")) \
        .withColumn("sbif_bal_no_rep_sign", F.lit(" ")) \
        .withColumn("sbif_bal_no_rep", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("sbif_tipo_tasa", F.lit("0000")) \
        .withColumn("sbif_prod_trans", F.lit("00")) \
        .withColumn("sbif_tipo_oper_trans", F.lit("0")) \
        .withColumn("lcy_fee_amt_sign", F.lit("+")) \
        .withColumn("orig_strt_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("nacc_from_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("pdue_from_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("wrof_from_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("orig_con_no", F.rpad(F.lit(" "), 30, " ")) \
        .withColumn("no_of_remn_coup", F.rpad(F.lit("0"), 4, "0")) \
        .withColumn("no_of_pdo_coup", F.rpad(F.lit("0"), 4, "0")) \
        .withColumn("no_of_tot_coup", F.rpad(F.lit("0"), 4, "0")) \
        .withColumn("sbif_dest_coloc", F.rpad(F.lit("0"), 3, "0")) \
        .withColumn("stop_accr_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("lst_int_pymt_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("ren_ind", F.lit(" ")) \
        .withColumn("lst_rset_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("next_rt_ch_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("lst_rt_ch_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("ocy_orig_nom_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("balAvl", F.when(F.col("cta_BLOCKEDBALANCE").isNull(), F.lpad(F.lit("0.00"), 20, "0"))
                                .otherwise(F.lpad(F.round(F.col("cta_BLOCKEDBALANCE"), 2).cast("string"), 20, "0"))) \
        .withColumn("lcy_pdo1_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lcy_pdo2_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("Lcy_pdo3_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lcy_oper_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("loc", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lcy_mnpy", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("lgl_actn_ind", F.lit(" ")) \
        .withColumn("Lcy_mv", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("Lcy_par_val", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("Port_typ", F.lit("0")) \
        .withColumn("No_rng", F.lit("000")) \
        .withColumn("Pdc_coup", F.lit("00000")) \
        .withColumn("Pgo_amt", F.lpad(F.lit("0.0000"), 20, "0")) \
        .withColumn("con_no_typ", F.lit(" ")) \
        .withColumn("ope_typ", F.lit(" ")) \
        .withColumn("mod_entr_bs", F.lit("S ")) \
        .withColumn("opc_compra", F.lpad(F.lit("0.00"), 13, "0")) \
        .withColumn("ident_instr", F.rpad(F.lit(" "), 5, " ")) \
        .withColumn("ident_emi_instr", F.rpad(F.lit(" "), 25, " ")) \
        .withColumn("serie_instr", F.rpad(F.lit(" "), 4, " ")) \
        .withColumn("subserie_instr", F.rpad(F.lit(" "), 5, " ")) \
        .withColumn("cat_risk_instr", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("limit_rate", F.lpad(F.lit("0.00000000"), 17, "0")) \
        .withColumn("pdc_after_fix_per", F.lpad(F.lit("0"), 4, "0")) \
        .withColumn("lcy_pdo4_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("lcy_pdo5_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("lcy_pdo6_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("sbif_no_rep_ind", F.lit("S")) \
        .withColumn("Lcy_otr_cont_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("lcy_pdo7_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("lcy_pdo8_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("lcy_pdo9_amt", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("assets_origin", F.lpad(F.lit("0"), 1, "0")) \
        .withColumn("first_expiry_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("tip_otorg", F.lit(" ")) \
        .withColumn("price_viv", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("tip_op_reneg", F.lit(" ")) \
        .withColumn("mon_pie_pag_reneg", F.lpad(F.lit("0"), 19, "0")) \
        .withColumn("seg_rem_cred_hip", F.lit(" ")) \
        .withColumn("pdue_from_oldest", F.lpad(F.lit("0"), 8, "0")) \
        .withColumn("mon_prev_rng", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("exig_pago", F.lit(" ")) \
        .withColumn("bidding_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("loan_disbursement_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("Accounting_dt", F.when(F.col("cfvfchap_VALUE").isNull(), F.rpad(F.lit(" "), 8, " "))
                                        .otherwise(F.rpad(F.date_format(F.col("cfvfchap_VALUE"), "yyyyMMdd"), 8, " "))) \
        .withColumn("last_payment_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("last_amount_paid", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("credit_line_approved_dt", F.rpad(F.lit(" "), 8, " ")) \
        .withColumn("Amount_instalment", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("Amount_revolving", F.lpad(F.lit("0.00"), 20, "0")) \
        .withColumn("Ind_credit_line_duration", F.lit(" ")) \
        .withColumn("nat_con_no", F.lpad(F.lit("12"), 4, " ")) \
        .withColumn("dest_finan", F.lit(" ")) \
        .withColumn("no_post_coup", F.lpad(F.lit("0"), 3, "0")) \
        .withColumn("giro", F.lpad(F.lit(" "), 2, " ")) \
        .withColumn("sbif_cust_act_eco", F.rpad(F.lit(" "), 10, " "))
    
    final_result_dropped = final_result.drop('cfvfcont_VALUE')
    final_result.show()

    return final_result_dropped
"""

def main(date):

    fecha_formateada = fecha(date)
    logger.info(f"La fecha de curse es: {fecha_formateada}")

    firstDateStr = first_month_day(fecha_formateada)
    logger.info(f"La fecha de primer día del mes en proceso es: {firstDateStr}")

    nextDay = next_acc_day(fecha_formateada)

    queries = get_table_names(GLUE_CUSTOM)

    dataframes = load_dataframes(queries, data_connect)

    result = result_df(dataframes, fecha_formateada, nextDay, GLUE_BANCOESTADO)

    #result.show()
    logger.info(f"Las columnas son: {result.columns}")
    logger.info(f"El resultado es: {result.show()}")

    #result.printSchema()

    #logger.info(f"Las columnas son {result.columns}")
    #logger.info(f"El esquema es: {result.printSchema()}")

    #final_result_dropped = logicaNegocio(result)
    
    #total_registros = final_result_dropped.count()

    result_pdf = result.toPandas()

    #text = f"099, {date}, {total_registros.rjust(8,'0')}"

    # Create a DataFrame with a single row containing the text
    #additional_row = Row(text=text)
    #additional_df = spark.createDataFrame([additional_row])

    #result_pdf.union(additional_df)

    result_pdf.to_csv('extractorcartera_output.csv')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process date.")
    parser.add_argument("--date", type=str, help="Date parameter.")
    args = parser.parse_args()

    if args.date is None:
        print("Please provide a date parameter using the --date argument.")
    else:
        main(args.date)
