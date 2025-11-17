from kafka import KafkaConsumer
import json
class EnergyConsumer :
  def __init__(self , topic = 'energy_reading' , bootstrap_servers='localhost:9092' ,group_id="energy_group"):
    self.consumer = KafkaConsumer(
      topic,
      bootstrap_servers = bootstrap_servers,
      auto_offset_reset = 'earliest', # for read from first if first time
      group_id = group_id,
      value_deserializer = lambda v:json.loads(v.decode('utf-8'))
    )
    
  def consume_data(self , n_message=10):
    count=0
    for message in self.consumer:
      print(f"Recived : {message.value}")
      count+=1
      if n_message and count>=n_message:
        break

if __name__ == "__main__":
    consumer = EnergyConsumer()
    consumer.consume_data(n_message=20)