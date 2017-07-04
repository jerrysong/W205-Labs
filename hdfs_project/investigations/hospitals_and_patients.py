import pyspark.sql.functions
from pyspark import SparkContext
from pyspark.sql import HiveContext

hive_context = HiveContext(SparkContext(master = 'local[*]'))

# Load the tables generated from the previous step from hive
hospitals = hive_context.table("hospitals")
procedures = hive_context.table("procedures")
surveys_responses = hive_context.table("surveys_responses")

# Join the hospitals table and procedures table. 
hos_pro = hospitals.join(procedures, 'hospital_id').select(hospitals.hospital_id, procedures.procedure_score)

# Join the above table and the surveys_responses table
hos_pro_res = hos_pro.join(surveys_responses, 'hospital_id').select(hos_pro.hospital_id, hos_pro.procedure_score, surveys_responses.survey_score)
hos_pro_res.registerTempTable("hos_pro_res")

# Group the data by hospital_id to get the average score for each hospital
hos_pro_res = hive_context.sql("select hospital_id, survey_score, avg(procedure_score) as average_score from hos_pro_res group by hospital_id, survey_score")

# Rank the hospitals in terms of average score, and output the top 10 results.
print '------------Top 10 Hospitals with Best Survey Responses------------'
hos_pro_res.orderBy(pyspark.sql.functions.desc('survey_score')).show(10)

corr = hos_pro_res.corr('survey_score', 'average_score')
print 'The correlation between survery score and hospital score is %s.' % corr
print 'It seems that there is negative relationship between survery score and hospital quality.'
