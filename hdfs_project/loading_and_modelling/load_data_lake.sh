# Download the data source files
wget https://www.dropbox.com/s/344ve98v5m3xtg1/Hospital%20General%20Information.csv
wget https://www.dropbox.com/s/efkmuarmoy9374a/hvbp_hcahps_05_28_2015.csv
wget https://www.dropbox.com/s/dzahompndvksuyc/Timely%20and%20Effective%20Care%20-%20Hospital.csv

# Strip the first line and rename
tail -n +2 "Hospital General Information.csv" > hospitals.csv
tail -n +2 "Timely and Effective Care - Hospital.csv" > effective_care.csv
tail -n +2 "hvbp_hcahps_05_28_2015.csv" > surveys_responses.csv

# Remove the original data files
rm "Hospital General Information.csv"
rm "Timely and Effective Care - Hospital.csv"
rm "hvbp_hcahps_05_28_2015.csv"

# Create the hdfs directories if not existing
hdfs dfs -mkdir -p /user/w205/surveys_responses_compare
hdfs dfs -mkdir -p /user/w205/effective_care_compare
hdfs dfs -mkdir -p /user/w205/hospitals_compare

# Put the data files to the hdfs
hdfs dfs -put hospitals.csv /user/w205/hospitals_compare
hdfs dfs -put effective_care.csv /user/w205/effective_care_compare
hdfs dfs -put surveys_responses.csv /user/w205/surveys_responses_compare
