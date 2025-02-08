from kafka.consumer import KafkaConsumer
from kafka.producer import KafkaProducer

from configs import kafka_config

class Reader:
    def __init__(self, measurements_topic_name,
                 temp_alert_topic_name,
                 humid_alert_topic_name):
        self.consumer = KafkaConsumer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_deserializer=lambda v: str(v.decode('utf-8')),
            key_deserializer=lambda v: str(v.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my_consumer_group_3'
        )
        self.consumer.subscribe([measurements_topic_name])

        self.temp_alert_producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_serializer=lambda v: str(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8')
        )
        self.humid_alert_producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_serializer=lambda v: str(v).encode('utf-8'),
            key_serializer=lambda v: str(v).encode('utf-8')
        )
        self.temp_alert_topic_name = temp_alert_topic_name
        self.humid_alert_topic_name = humid_alert_topic_name

        self.temp_normal_range = (-1000, 40)
        self.humid_normal_range = (20, 80)

    def read(self):
        try:
            while True:
                for message in self.consumer:
                    print(f"Received message: {message.value}")
                    temp = int(message.value.split(",")[2].split(":")[1])
                    humid = int(message.value.split(",")[3].split(":")[1])
                    if temp < self.temp_normal_range[0] or temp > self.temp_normal_range[1]:
                        self.send_alert('temp', message.value)
                    if humid < self.humid_normal_range[0] or humid > self.humid_normal_range[1]:
                        self.send_alert('humid', message.value)
        except KeyboardInterrupt as e:
            print("Reading stopped.")
        finally:
            self.consumer.close()

    def send_alert(self, alert_type, message):
        if alert_type == 'temp':
            message += ", Alert: Temperature is out of normal range!"
            self.temp_alert_producer.send(self.temp_alert_topic_name, key='temp', value=message)
            self.temp_alert_producer.flush()
            print(f"Temperature alert sent to topic '{self.temp_alert_topic_name}' successfully with message: {message}")
        elif alert_type == 'humid':
            message += ", Alert: Humidity is out of normal range!"
            self.humid_alert_producer.send(self.humid_alert_topic_name, key='humid', value=message)
            self.humid_alert_producer.flush()
            print(f"Humidity alert sent to topic '{self.humid_alert_topic_name}' successfully with message: {message}")


if __name__ == "__main__":
    my_name = "staskut"
    measurements_topic_name = f'{my_name}_building_sensors'
    temp_alert_topic_name = f'{my_name}_temperature_alerts'
    humid_alert_topic_name = f'{my_name}_humidity_alerts'

    reader = Reader(measurements_topic_name, temp_alert_topic_name, humid_alert_topic_name)
    reader.read()