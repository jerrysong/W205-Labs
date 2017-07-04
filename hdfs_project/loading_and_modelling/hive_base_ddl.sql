DROP TABLE IF EXISTS hospitals_raw;
CREATE EXTERNAL TABLE hospitals_raw
(Provider_Id string,
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code string,
County_Name string,
Phone_Number string,
Hospital_Type string,
Hospital_Ownership string,
Emergency_Services string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospitals_compare';

DROP TABLE IF EXISTS procedures_raw;
CREATE EXTERNAL TABLE procedures_raw
(Provider_Id string,
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code string,
County_Name string,
Phone_Number string,
Condition string,
Measure_Id string,
Measure_Name string,
Score string,
Sample string,
Footnote string,
Measure_Start_Date string,
Measure_End_Date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/effective_care_compare';

DROP TABLE IF EXISTS surveys_responses_raw;
CREATE EXTERNAL TABLE surveys_responses_raw
(Provider_Id string,
Hospital_Name string,
Address string,
City string,
State string,
ZIP_Code string,
County_Name string,
Comm_Nurses_Achv_Points string,
Comm_Nurses_Imp_Points string,
Comm_Nurses_Dim_Score string,
Comm_Doctors_Achv_Points string,
Comm_Doctors_Imp_Points string,
Comm_Doctors_Dim_Score string,
Resp_Achv_Points string,
Resp_Imp_Points string,
Resp_Dim_Score string,
Pain_Management_Achv_Points string,
Pain_Management_Imp_Points string,
Pain_Management_Dim_Score string,
Comm_Medi_Achv_Points string,
Comm_Medi_Imp_Points string,
Comm_Medi_Dim_Score string,
Cleanliness_Achv_Points string,
Cleanliness_Imp_Points string,
Cleanliness_Dim_Score string,
Discharge_Info_Achv_Points string,
Discharge_Info_Imp_Points string,
Discharge_Info_Dim_Score string,
Overall_Achv_Points string,
Overall_Imp_Points string,
Overall_Dim_Score string,
Hcahps_Base_Score string,
Hcahps_Consistency_Score string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/surveys_responses_compare';
