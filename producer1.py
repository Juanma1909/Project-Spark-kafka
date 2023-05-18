from kafka import KafkaProducer
from json import dumps
import time
import random
import datetime
from datetime import datetime, timedelta


# Set up Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer = lambda x: dumps(x).encode('utf-8'))

# Define Kafka topic
topic = 'final1'
dishes = ["spaghetti carbonara", "lasagna", "chicken parmesan", "grilled salmon", "hamburger", "steak frites", "chicken alfredo", "beef stroganoff", "vegetable stir-fry", "taco salad", "sushi roll", "caesar salad", "roast beef sandwich", "chicken tikka masala", "mushroom risotto"]
start_time = datetime(2023, 1, 1, 13, 0, 0)
end_time = datetime(2023, 1, 1, 17, 0, 0)
time_range = end_time - start_time
for i in range(40):
	data = {"dish":random.choice(dishes), "hour":str((start_time + timedelta( seconds = random.randint(0,time_range.seconds))).time()),"codCity":1000+((random.randint(1,4))) ,"amount": random.randint(1,4)}
	print(data)
	producer.send(topic,value = data)
	time.sleep(1)
