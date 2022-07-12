import json
from kafka import KafkaConsumer
from sqlalchemy import create_engine
import pandas as pd 



if __name__ == "__main__":
    ## create sql engine/ connect to DB
    engine = create_engine('postgresql://postgres:221299@localhost:5432/digitalskola')
    
    ## setup connection to kafka consumer
    consumer = KafkaConsumer("digitalskola2", bootstrap_servers ="localhost")
    print("starting the consumer")

    ## read data in kafka consumer
    for msg in consumer:
        ## memasukan data/messasge dengan format json
        print(f"Record = {json.loads(msg.value)}")

        ## declaration data di dalam consumer
        data = json.loads(msg.value)

        ## make data to Dataframe
        df = pd.DataFrame(data,index = [0])
        print(df)

        ## sending data to DB
        df.to_sql('user_activity', engine, if_exists='append',index=False)