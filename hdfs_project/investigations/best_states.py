import pyspark.sql.functions
from pyspark import SparkContext
from pyspark.sql import HiveContext

hive_context = HiveContext(SparkContext(master = 'local[*]'))

# Load the tables generated from the previous step from hive
hospitals = hive_context.table("hospitals")
procedures = hive_context.table("procedures")

# Join these two tables. We will use the average score of a state's all hospitals to measure the state's hospital quality
hos_pro = hospitals.join(procedures, 'hospital_id').select(hospitals.hospital_id, hospitals.state_name, procedures.procedure_score).orderBy('hospital_id')
hos_pro.registerTempTable("hos_pro")

# Group the data by hospital_id
hos_pro = hive_context.sql("select hospital_id, state_name, avg(procedure_score) as hospital_score from hos_pro group by hospital_id, state_name")
hos_pro.registerTempTable("hos_pro")

# Group the data by state_name and calculate the related statistics.
hos_pro = hive_context.sql("select state_name, avg(hospital_score) as average_score, stddev(hospital_score) as standard_deviation, sum(hospital_score) as score_sum from hos_pro group by state_name")

# Rank the states in terms of average score, and output the top 10 results.
print '------------Top 10 Best States------------'
hos_pro.orderBy(pyspark.sql.functions.desc('average_score')).show(10)

