from kafka import KafkaProducer
import json
import csv
import time
import os

class Producer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def start_streaming(self, file1_path, file2_path, topic1, topic2 , limit = 100):
        print(f"---Starting Stream from:\n 1. {file1_path}\n 2. {file2_path}\n---")
        
        try:
            with open(file1_path, 'r', encoding='utf-8') as f1 , open(file2_path, 'r', encoding='utf-8') as f2:
                reader1 = csv.DictReader(f1)
                reader2 = csv.DictReader(f2)
                
                counter = 0
                
                for r1, r2 in zip(reader1, reader2):
                    
                    if 'Device' not in r1 or not r1['Device']:
                      r1['Device'] = 'Unknown_Device'
                    
                    r2['Timestamp'] = r1['Timestamp']
                    r2['House_ID'] = r1['House_ID']

                    self.producer.send(topic1, value=r1)
                    print(f"Send to topic name : {topic1} = {r1}")
                    self.producer.send(topic2, value=r2)
                    print(f"Send to topic name : {topic2} = {r2}")
                    
                    counter += 1
                    if counter > limit :
                      break
                    
                    time.sleep(0.3)
            
            self.producer.flush()
            print("\n --- Done Sending All Data ---")

        except FileNotFoundError:
            print(f"\nError In file Path")

if __name__ == "__main__":
    producer = Producer()

    path_1 = '../DataSet/Data_Set_1_DeviceMeasurment.csv'
    path_2 = '../DataSet/Data_Set_2_Flags.csv'
    
    topic_1 = 'topic-file-1'
    topic_2 = 'topic-file-2'
    
    input("Press Enter to start streaming...")
    producer.start_streaming(path_1, path_2, topic_1, topic_2)