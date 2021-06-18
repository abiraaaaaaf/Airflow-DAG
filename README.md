# Airflow DAG
 
- A pipeline using airflow to train a ML regression model
- A DAG code to do the following
   - T1. A task to download all training csv from s3 bucket and store locally.
   - T2. A task to read all the downloaded csv and train the model and finally save the model locally.
   - T3. A task to download prediction.csv from S3 save it locally.
   - T4. Load the local model and read the downloaded prediction CSV and save a csv with prediction of Species for each input row in prediction.csv.


                    ![DAG](https://user-images.githubusercontent.com/38458092/122529505-ceb76600-d032-11eb-84ba-7230f376b48b.JPG)
