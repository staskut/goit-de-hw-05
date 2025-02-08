The script `create_topics.py` creates Kafka topics:
![1_topics_created.png](screenshots/1_topics_created.png)
The script `run_sensor.py` initializes a sensor and sends data to the Kafka topic:
![2_running_sensors.png](screenshots/2_running_sensors.png)
The script `read_measurements.py` reads data from the Kafka topic and sends alerts to the corresponding topics if required (temp>40 or hum>80 or hum<20):
![3_send_alerts.png](screenshots/3_send_alerts.png)
The script `read_alerts.py` reads alerts from the Kafka topics:
![4_read_alerts.png](screenshots/4_read_alerts.png)