import pyspark.sql.functions
from pyspark import SparkContext
from pyspark.sql import HiveContext

hive_context = HiveContext(SparkContext(master = 'local[*]'))

# Load tables from hive and only select the necessary columns 
hospitals = hive_context.sql("select provider_id as hospital_id, hospital_name, state as state_name from hospitals_raw order by hospital_id")
procedures = hive_context.sql("select measure_id as procedure_id, measure_name as procedure_name, provider_id as hospital_id, score as procedure_score from procedures_raw order by measure_id")
surveys_responses = hive_context.sql("select provider_id as hospital_id, hcahps_base_score as survey_score from surveys_responses_raw order by hospital_id")

# Clean the score column in procedures and surveys_responses table. Remove all non-integer cells and cast the column type to int.
procedures = procedures.filter(procedures['procedure_score'].rlike('^\d+$'))
procedures = procedures.withColumn('procedure_score', procedures['procedure_score'].cast("int"))
surveys_responses = surveys_responses.filter(surveys_responses['survey_score'].rlike('^\d+$'))
surveys_responses = surveys_responses.withColumn('survey_score', surveys_responses['survey_score'].cast("int"))

# Left join the hospitals table with the procedures table to get the procedure id column.
hospitals = hospitals.join(procedures, 'hospital_id').select(hospitals.hospital_id, hospitals.hospital_name, hospitals.state_name, procedures.procedure_id).orderBy('hospital_id')

# Save the transformed tables to hive permanently.
hospitals.saveAsTable('hospitals')
procedures.saveAsTable('procedures')
surveys_responses.saveAsTable('surveys_responses')
