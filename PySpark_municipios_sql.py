# code utf-8

# Importación de librerias
# from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, rtrim, expr
from pyspark.sql.types import  StructField, StructType, StringType, IntegerType, DoubleType, FloatType

sparkSQL = SparkSession \
.builder \
.appName("SparkSQL") \
.master("local") \
.getOrCreate()

###Municipios###

schema_mun = StructType([
    StructField("codigo", IntegerType(), False),
    StructField("comunidad", StringType(), False),
    StructField("provincia", StringType(), False),
    StructField("municipio", StringType(), False),
    StructField("poblacion", IntegerType(), False),
])

df_mun = sparkSQL.read\
    .option("header", True)\
    .option("delimiter", ";")\
    .schema(schema_mun) \
    .csv("hdfs://localhost:9000/ficheros/PECMunicipios.csv")

df_mun.printSchema()
df_mun.show()




###Elecciones###
#Creación del esquema
schema_elec = StructType([
StructField('codigo', StringType(), False),
StructField('Mesas', StringType(), False),
StructField('Censo', StringType(), False),
StructField('Votantes', StringType(), False),
StructField('Validos', StringType(), False),
StructField('Blanco', StringType(), False),
StructField('Nulos', StringType(), False),
StructField('PP', StringType(), False),
StructField('PSOE', StringType(), False),
StructField('PODEMOS-IU-EQUO', StringType(), False),
StructField('CIUDADANOS', StringType(), False),
StructField('ECP', StringType(), False),
StructField('PODEMOS-COMPROMIS-EUPV', StringType(), False),
StructField('ERC-CATSI', StringType(), False),
StructField('CDC', StringType(), False),
StructField('PODEMOS-EN MAREA-ANOVA-EU', StringType(), False),
StructField('EAJ-PNV', StringType(), False),
StructField('EH-BILDU', StringType(), False),
StructField('CCA-PNC', StringType(), False),
StructField('PACMA', StringType(), False),
StructField('RECORTES CERO-GRUPO VERDE', StringType(), False),
StructField('UPYD', StringType(), False),
StructField('VOX', StringType(), False),
StructField('BNG-NOS', StringType(), False),
StructField('PCPE', StringType(), False),
StructField('GBAI', StringType(), False),
StructField('EB', StringType(), False),
StructField('FE DE LAS JONS', StringType(), False),
StructField('SI', StringType(), False),
StructField('SOMVAL', StringType(), False),
StructField('CCD', StringType(), False),
StructField('SAIN', StringType(), False),
StructField('PH', StringType(), False),
StructField('CENTRO MODERADO', StringType(), False),
StructField('P-LIB', StringType(), False),
StructField('CCD-CI', StringType(), False),
StructField('UPL', StringType(), False),
StructField('PCOE', StringType(), False),
StructField('AND', StringType(), False),
StructField('JXC', StringType(), False),
StructField('PFYV', StringType(), False),
StructField('CILUS', StringType(), False),
StructField('PXC', StringType(), False),
StructField('MAS', StringType(), False),
StructField('IZAR', StringType(), False),
StructField('UNIDAD DEL PUEBLO', StringType(), False),
StructField('PREPAL', StringType(), False),
StructField('LN', StringType(), False),
StructField('REPO', StringType(), False),
StructField('INDEPENDIENTES-FIA', StringType(), False),
StructField('ENTABAN', StringType(), False),
StructField('IMC', StringType(), False),
StructField('PUEDE', StringType(), False),
StructField('FE', StringType(), False),
StructField('ALCD', StringType(), False),
StructField('FME', StringType(), False),
StructField('HRTS-LN', StringType(), False),
StructField('UDT', StringType(), False)
])


df_elec = sparkSQL.read\
    .option("header", True)\
    .option("delimiter", ";")\
    .schema(schema_elec) \
    .csv("hdfs://localhost:9000/ficheros/PECElecciones.csv")

df_elec.printSchema()
df_elec.show()

# withColumns --> Crear coli
# .join -->

df_joined = df_mun.join(df_elec, df_mun.codigo == df_elec.codigo, "inner")\
    .show(truncate=False)