from kafka import KafkaProducer
import json
import random
import time

class EnergyProducer:
  def __init__(self , topic='energy_reading' , bootstrap_servers='localhost:9092' , devices_file='devices.json'):
    self.producer = KafkaProducer(
      bootstrap_servers = bootstrap_servers,
      #value_serializer → بيحوّل أي بيانات Python (dictionary) إلى JSON string ويشفّرها byte string قبل الإرسال، لأن Kafka بيقبل بس byte.
      value_serializer = lambda v: json.dumps(v).encode('utf-8')
    )
    self.topic = topic

    with open(devices_file,'r') as f:
      self.devices = json.load(f)['devices']

  def generateData(self) :
    return {
      'devices' : random.choice(self.devices),
      'consumption' : random.randint(50,500),
      'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }
  
  def sendData(self,num_message=10,interval=1):
    for _ in range(num_message):
      data = self.generateData()
      self.producer.send(self.topic,value=data)
      print(f"Sent: {data}")
      time.sleep(interval)
    self.producer.flush()

if __name__ == "__main__":
  producer = EnergyProducer()
  producer.sendData(num_message=20 , interval=1)