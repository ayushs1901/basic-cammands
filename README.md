# basic-cammands

========================================
AIOPS EXAM — COMMAND CHEAT SHEET
========================================

1. PYTHON / ENVIRONMENT
========================================

Check Python:
python --version

Check Python 3:
python3 --version

Check pip:
python -m pip --version

Create virtual environment:
python -m venv .venv

Activate environment (Linux / Codespaces):
source .venv/bin/activate

Deactivate:
deactivate

Install package:
python -m pip install PACKAGE_NAME

Examples:
python -m pip install pandas
python -m pip install matplotlib
python -m pip install kafka-python

Install multiple:
python -m pip install pandas matplotlib kafka-python

Show installed packages:
pip list

Run Python file:
python filename.py

Examples:
python q1_anomaly.py
python topic.py
python producer.py
python consumer.py

Test package:
python -c "import pandas; print('pandas OK')"
python -c "import matplotlib; print('matplotlib OK')"
python -c "import kafka; print('kafka OK')"


========================================
2. BASIC TERMINAL COMMANDS
========================================

Show current directory:
pwd

List files:
ls

Enter folder:
cd folder_name

Go back:
cd ..

Create folder:
mkdir folder_name

Create file:
touch filename.py

Clear terminal:
clear


========================================
3. GIT — VERY IMPORTANT
========================================

Check changes:
git status

Add one file:
git add filename.py

Add all changes:
git add .

Commit:
git commit -m "message"

Push to GitHub:
git push

Get latest changes:
git pull

See branches:
git branch

Create branch:
git branch branch_name

Switch branch:
git switch branch_name

Create + switch branch:
git switch -c branch_name

Alternative:
git checkout branch_name

Create + switch:
git checkout -b branch_name

Merge:
git merge branch_name


MOST IMPORTANT GIT WORKFLOW:

git status
git add .
git commit -m "Complete task"
git push


========================================
4. GIT CONCEPTS
========================================

COMMIT
= Save changes in Git history.

PUSH
= Upload local commits to GitHub.

PULL
= Download latest changes from GitHub.

BRANCH
= Separate line of development.

MERGE
= Combine one branch into another.

PULL REQUEST
= Request to merge changes from one branch
into another through GitHub.


Typical workflow:

Create branch
      ↓
Write code
      ↓
git add .
      ↓
git commit -m "message"
      ↓
git push
      ↓
Create Pull Request on GitHub


========================================
5. PYTHON — AIOPS
========================================

Import Pandas:
import pandas as pd

Import Matplotlib:
import matplotlib.pyplot as plt

Create DataFrame:
df = pd.DataFrame(data)

Number of records:
len(df)

First rows:
df.head()

Statistics:
df.describe()

Average:
df["CPU"].mean()

Maximum:
df["CPU"].max()

Minimum:
df["CPU"].min()

Filter:
df[df["CPU"] > 80]

Store anomalies:
anomalies = df[df["CPU"] > 80]

Count anomalies:
len(anomalies)

Print:
print(anomalies)


========================================
6. MATPLOTLIB
========================================

Basic graph:

plt.plot(df["Timestamp"], df["CPU"])

Label X:
plt.xlabel("Timestamp")

Label Y:
plt.ylabel("CPU Usage")

Title:
plt.title("CPU Usage Over Time")

Display:
plt.show()


Complete basic graph:

plt.plot(df["Timestamp"], df["CPU"], marker="o")
plt.xlabel("Timestamp")
plt.ylabel("CPU Usage")
plt.title("CPU Usage Over Time")
plt.show()


========================================
7. ANOMALY DETECTION
========================================

Basic threshold:

if cpu > 80:
    print("ANOMALY")

Pandas threshold:

anomalies = df[df["CPU"] > 80]

Count:

print("Anomalies detected:", len(anomalies))


Counter:

anomaly_count = 0

if cpu > 80:
    anomaly_count += 1


Final count:

print("Total anomalies detected:", anomaly_count)


========================================
8. PYTHON DICTIONARY
========================================

Example:

message = {
    "server_id": "server01",
    "cpu_usage": 85,
    "memory_usage": 62
}

Access values:

message["server_id"]

message["cpu_usage"]

message["memory_usage"]


========================================
9. JSON
========================================

Import:
import json

Python dictionary → JSON:

json.dumps(data)

JSON → Python dictionary:

json.loads(data)

Encode:

data.encode("utf-8")

Decode:

data.decode("utf-8")


Kafka producer commonly uses:

json.dumps(x).encode("utf-8")


Kafka consumer commonly uses:

json.loads(value.decode("utf-8"))


========================================
10. KAFKA — BASIC CONCEPT
========================================

Producer
    ↓
Kafka Broker
    ↓
Topic
    ↓
Consumer


Producer
= Sends messages.

Consumer
= Receives messages.

Broker
= Kafka server.

Topic
= Named channel/category for messages.

Partition
= Division of a topic.

Cluster
= Collection of Kafka brokers.

Consumer Group
= Group of consumers working together.


========================================
11. KAFKA TOPIC — topic.py
========================================

Code:

from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9092"
)

topic = NewTopic(
    name="server_metrics",
    num_partitions=1,
    replication_factor=1
)

admin.create_topics(new_topics=[topic])

print("Topic created successfully!")

admin.close()


IMPORTANT:

KafkaAdminClient(...)
= Connect to Kafka for administration.

bootstrap_servers="localhost:9092"
= Kafka broker address.

NewTopic(...)
= Defines a new Kafka topic.

name="server_metrics"
= Topic name.

num_partitions=1
= One partition.

replication_factor=1
= One replica.

admin.create_topics(...)
= Creates the topic.

admin.close()
= Closes admin connection.


========================================
12. KAFKA PRODUCER — producer.py
========================================

Code:

from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

for i in range(10):

    message = {
        "server_id": f"server{i+1}",
        "cpu_usage": 50 + i * 4,
        "memory_usage": 60 + i
    }

    producer.send(
        "server_metrics",
        value=message
    )

    print("Sent:", message)

    time.sleep(1)

producer.flush()
producer.close()


IMPORTANT:

KafkaProducer(...)
= Creates producer.

bootstrap_servers
= Kafka broker location.

value_serializer
= Converts Python data into bytes/JSON.

for i in range(10)
= Send 10 messages.

message
= Server metric dictionary.

producer.send(...)
= Sends message to Kafka topic.

"server_metrics"
= Topic name.

value=message
= Actual message being sent.

time.sleep(1)
= Wait one second.

producer.flush()
= Make sure pending messages are sent.

producer.close()
= Close producer.


========================================
13. KAFKA CONSUMER — consumer.py
========================================

Code:

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "server_metrics",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="aiops-monitor",
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)

print("Waiting for messages...")

for message in consumer:

    data = message.value

    server = data["server_id"]
    cpu = data["cpu_usage"]
    memory = data["memory_usage"]

    print("\nReceived:")
    print("Server:", server)
    print("CPU:", cpu, "%")
    print("Memory:", memory, "%")

    if cpu > 80:
        print("ALERT: High CPU detected on", server)


IMPORTANT:

KafkaConsumer(...)
= Creates consumer.

"server_metrics"
= Topic to consume from.

bootstrap_servers
= Kafka broker address.

auto_offset_reset="earliest"
= Start from earliest available messages
when appropriate.

enable_auto_commit=True
= Automatically commit consumer offsets.

group_id="aiops-monitor"
= Consumer group name.

value_deserializer
= Converts Kafka data back into Python data.

for message in consumer:
= Continuously receive messages.

message.value
= Actual message data.

data["server_id"]
= Get server ID.

data["cpu_usage"]
= Get CPU.

data["memory_usage"]
= Get memory.


========================================
14. KAFKA ANOMALY ALERT
========================================

Basic:

if cpu > 80:
    print("ALERT: High CPU detected")


With server:

if cpu > 80:
    print("ALERT: High CPU detected on", server)


With counter:

anomaly_count = 0

if cpu > 80:
    anomaly_count += 1
    print("ALERT: High CPU detected")


Final:

print("Total anomalies detected:", anomaly_count)


========================================
15. AIRFLOW — BASIC CONCEPTS
========================================

Airflow
= Workflow orchestration tool.

DAG
= Directed Acyclic Graph.

Task
= One unit of work.

PythonOperator
= Runs a Python function as an Airflow task.

Dependency
= Defines which task runs before another.


Basic workflow:

collect_metrics
       ↓
process_metrics
       ↓
detect_anomaly
       ↓
generate_report


Dependency syntax:

collect_task >> process_task

Complete:

collect_task >> process_task >> detect_task >> report_task


========================================
16. AIRFLOW PYTHONOPERATOR
========================================

Import:

from airflow.operators.python import PythonOperator

Function:

def collect_metrics():
    print("Collecting metrics")

Task:

collect_task = PythonOperator(
    task_id="collect_metrics",
    python_callable=collect_metrics
)


IMPORTANT:

task_id
= Unique task name.

python_callable
= Python function that the task runs.


========================================
17. AIRFLOW BASIC DAG
========================================

Basic structure:

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def collect_metrics():
    print("Collecting metrics")


def process_metrics():
    print("Processing metrics")


def detect_anomaly():
    print("Detecting anomaly")


def generate_report():
    print("Generating report")


with DAG(
    dag_id="aiops_workflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    collect_task = PythonOperator(
        task_id="collect_metrics",
        python_callable=collect_metrics
    )

    process_task = PythonOperator(
        task_id="process_metrics",
        python_callable=process_metrics
    )

    detect_task = PythonOperator(
        task_id="detect_anomaly",
        python_callable=detect_anomaly
    )

    report_task = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report
    )

    collect_task >> process_task >> detect_task >> report_task


========================================
18. AIRFLOW COMMANDS
========================================

Check Airflow:

airflow version

List DAGs:

airflow dags list


IMPORTANT:
The exact Airflow CLI commands available can depend
on the Airflow version/environment.

For the exam, focus mainly on:

DAG
PythonOperator
tasks
dependencies
Python functions


========================================
19. KAFKA TERMINAL COMMANDS
========================================

Know these names/concepts:

kafka-server-start.sh
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh


Possible operations:

Start Kafka:
kafka-server-start.sh ...

Create topic:
kafka-topics.sh ...

List topics:
kafka-topics.sh --list ...

Describe topic:
kafka-topics.sh --describe ...


IMPORTANT:
Do not focus heavily on Kafka installation/setup because
the teacher said setup is not required for the exam.


========================================
20. COMMON ERRORS
========================================

ModuleNotFoundError
→ Required Python package is missing.

Fix example:
python -m pip install pandas

SyntaxError
→ Python syntax is incorrect.

NameError
→ Variable/function name is wrong or not defined.

KeyError
→ Dictionary key does not exist.

Connection refused
→ Program cannot connect to Kafka broker.

Wrong topic name
→ Producer and consumer must use the correct topic.

Git merge conflict
→ Git cannot automatically combine changes.


========================================
21. EXAM QUESTION → WHAT TO USE
========================================

Dataset + CPU + Memory + Graph
→ Pandas + Matplotlib

Statistics
→ df.describe()

Average
→ df["CPU"].mean()

Anomaly
→ df[df["CPU"] > 80]

Kafka topic
→ KafkaAdminClient + NewTopic

Kafka producer
→ KafkaProducer + producer.send()

Kafka consumer
→ KafkaConsumer + for message in consumer

CPU alert
→ if cpu > 80

Anomaly count
→ anomaly_count += 1

Airflow workflow
→ DAG + PythonOperator

Task dependency
→ task1 >> task2

Git submission
→ git add → git commit → git push


========================================
22. FINAL EXAM WORKFLOW
========================================

FOR EVERY QUESTION:

1. Read the question carefully.

2. Identify the topic:

Python?
Kafka?
Airflow?
Git?

3. Create/open required file.

4. Write code.

5. Run/test code.

6. Fix errors.

7. Check:

git status

8. Add:

git add .

9. Commit:

git commit -m "Complete task"

10. Push:

git push


========================================
23. MOST IMPORTANT THINGS TO MEMORIZE
========================================

1.

python --version


2.

source .venv/bin/activate


3.

python -m pip install package


4.

python filename.py


5.

git status


6.

git add .


7.

git commit -m "message"


8.

git push


9.

anomalies = df[df["CPU"] > 80]


10.

producer.send("server_metrics", value=message)


11.

for message in consumer:
    data = message.value


12.

if cpu > 80:
    print("ALERT: High CPU detected")


13.

anomaly_count += 1


14.

PythonOperator(...)


15.

task1 >> task2 >> task3 >> task4


========================================
END OF CHEAT SHEET
========================================
