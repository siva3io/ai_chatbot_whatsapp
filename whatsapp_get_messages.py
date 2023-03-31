import requests, json
from kafka import KafkaProducer

const = 0
url = "https://eunimart.herokuapp.com/"
broker = '20.197.41.10:9092'
topics = ['private.ai.chatbot']
producer = KafkaProducer(bootstrap_servers=[broker], api_version=(0, 10, 1))
while True :
    response = requests.request("GET", url, headers={}, data={})
    response = json.loads(response.text.lstrip("<pre>").rstrip("</pre>"))
    if len(response) > const :
        for message in response[:len(response)-const]:
            message = json.dumps(message)
            print(message)
            producer.send(topics[0] , value=message.encode('utf-8'))
    const = len(response)
    print(const)
