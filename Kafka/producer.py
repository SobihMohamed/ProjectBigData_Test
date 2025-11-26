from kafka import KafkaProducer
import json
import csv
import time

class FileProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') 
        )

    def send_csv_to_kafka(self, file_path, topic_name, limit=100):
        print(f"Starting to send data from {file_path} to topic: {topic_name}")
        
        with open(file_path, 'r', encoding='utf-8') as f:

            reader = csv.DictReader(f)
            
            count = 0
            for row in reader:
                # senf to kafka
                self.producer.send(topic_name, value=row)
                print(f"Sent to {topic_name}: {row}")
                
                # for streaming 
                time.sleep(0.5) 
                
                count += 1
                # لو حاطط ليميت معين عشان التجربة
                if limit > 0 and count >= limit:
                    break
        
        self.producer.flush()
        print(f"Finished sending {count} records to {topic_name}")

if __name__ == "__main__":
    producer = FileProducer()

    path_file_1 = '../DataSet/Data_Set_1_DeviceMeasurment.csv'
    path_file_2 = '../DataSet/Data_Set_2_Flags.csv'

    input("Press Enter to start sending File 1...")
    producer.send_csv_to_kafka(file_path=path_file_1, topic_name='topic-file-1')

    input("Press Enter to start sending File 2...")
    producer.send_csv_to_kafka(file_path=path_file_2, topic_name='topic-file-2')