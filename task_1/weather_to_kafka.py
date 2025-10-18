# weather_to_kafka.py
import requests, time, json
from kafka import KafkaProducer

OWM_KEY = "e1470dc6a4ec0ab417186940eb595c2c"
cities = ["Hanoi","Ho Chi Minh City"]
producer = KafkaProducer(bootstrap_servers='localhost:9092')

for city in cities:
    r = requests.get("https://api.openweathermap.org/data/2.5/weather",
                     params={"q": city, "appid": OWM_KEY, "units": "metric"})
    if r.ok:
        payload = r.json()
        event = {
            "timestamp": payload.get("dt"),
            "city": city,
            "temp": payload['main']['temp'],
            "weather": payload['weather'][0]['description']
        }
        producer.send('weather', json.dumps(event).encode('utf-8'))
        print("Pushed weather for", city)
producer.flush()
producer.close()
