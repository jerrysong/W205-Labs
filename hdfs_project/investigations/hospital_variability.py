import pyspark.sql.functions
from pyspark import SparkContext
from pyspark.sql import HiveContext

hive_context = HiveContext(SparkContext(master = 'local[*]'))

# Load the tables generated from the previous step from hive
procedures = hive_context.table("procedures")

# Group the data by procedure_id and calculate the standard deviation. The sd will be used to measure the variability.
pro = hive_context.sql("select procedure_id, procedure_name, stddev(procedure_score) as standard_deviation from procedures group by procedure_id, procedure_name")

# Rank the procedures in terms of the standard deviation, and output the top 10 results.
print '---Top 10 Procedures with Greatest Variability---'
pro.orderBy(pyspark.sql.functions.desc('standard_deviation')).show(10)

