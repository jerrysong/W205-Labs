# Go to the project directory and prepare for the running 
cd <where you download the project>/ex2/exercise_2/tweetwordcount
. set_path
python create_db.py

# Run the app
sparse run

# Run queries
python src/psql/finalresults.py
python src/psql/histogram.py 1 10
