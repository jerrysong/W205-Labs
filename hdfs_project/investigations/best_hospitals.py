import pyspark.sql.functions
from pyspark import SparkContext
from pyspark.sql import HiveContext

hive_context = HiveContext(SparkContext(master = 'local[*]'))

# Load the tables generated from the previous step from hive
hospitals = hive_context.table("hospitals")
procedures = hive_context.table("procedures")

# Join this two tables. Because we have no knowledge about the weight of different procedure score, we will use the arithmetic mean score of a hospital's all procedures to measure the hospital's service quality. 
hos_pro = hospitals.join(procedures, 'hospital_id').select(hospitals.hospital_id, hospitals.hospital_name, procedures.procedure_score).orderBy('hospital_id')
hos_pro.registerTempTable("hos_pro")

# Group the data by hospital_id and calculate the related statistics 
hos_pro = hive_context.sql("select hospital_id, hospital_name, avg(procedure_score) as average_score, stddev(procedure_score) as standard_deviation, sum(procedure_score) as score_sum from hos_pro group by hospital_id, hospital_name")

# Rank the hospitals in terms of average score, and output the top 10 results.
print '------------Top 10 Best Hospitals------------'
hos_pro.orderBy(pyspark.sql.functions.desc('average_score')).show(10)

