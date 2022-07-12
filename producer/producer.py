import json
from kafka import KafkaProducer
import time

if __name__ == "__main__":
    ## read json data
    with open('logFraud.json', 'rb') as file:
        file = json.load(file)

    ## fungtion untuk mengidentifikasi bahwa datanya adalah json
    def json_serializer(data):
        return json.dumps(data).encode("utf-8")

     ## make connection to kafka/ setup connection to kafka producer
    producer = KafkaProducer(bootstrap_servers = ['localhost'],
                            value_serializer = json_serializer)
                                
    ## sending data to kafka
    while True:
        for data in file:
            print(data)

            ## sending data to kafka topic
            producer.send("digitalskola2", data)

            ## setting waktu untuk pengiriman data ke consumer
            time.sleep(10)


