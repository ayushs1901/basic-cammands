Yes. Based on **your teacher's instructions + the sample paper + the Kafka code your teacher gave you**, this is the command list I would learn tonight. The sample paper itself focuses on Python anomaly detection, Kafka topic/producer/consumer, Airflow DAG/PythonOperator/dependencies, and the integrated Kafka anomaly workflow. :chatgpt-content-reference{index="0"} :chatgpt-content-reference{index="1"} :chatgpt-content-reference{index="2"}

# 🔴 AIOps Exam — Commands to Memorize

Don't try to memorize every Linux command. There are **4 groups** you need.

---

# 1. Python / Environment Commands

### Check Python

```bash
python --version
```

If that doesn't work:

```bash
python3 --version
```

### Check pip

```bash
pip --version
```

or:

```bash
python -m pip --version
```

### Create virtual environment

Know this:

```bash
python -m venv .venv
```

### Activate virtual environment — Linux/Codespaces

```bash
source .venv/bin/activate
```

You'll usually see:

```text
(.venv)
```

in the terminal.

### Deactivate

```bash
deactivate
```

---

# 2. Installing Packages

Your teacher specifically said **install packages**, so know:

```bash
python -m pip install pandas
```

```bash
python -m pip install matplotlib
```

```bash
python -m pip install kafka-python
```

If you need multiple:

```bash
python -m pip install pandas matplotlib kafka-python
```

### Check installed packages

```bash
pip list
```

### Verify a package

```bash
python -c "import pandas; print('pandas OK')"
```

```bash
python -c "import matplotlib; print('matplotlib OK')"
```

```bash
python -c "import kafka; print('kafka OK')"
```

### Run Python file

```bash
python filename.py
```

Examples:

```bash
python q1_anomaly.py
```

```bash
python producer.py
```

```bash
python consumer.py
```

---

# 3. Basic Terminal Commands

These aren't AIOps-specific, but they can save you if you need to navigate the repository.

### Where am I?

```bash
pwd
```

### See files

```bash
ls
```

### Enter folder

```bash
cd folder_name
```

Example:

```bash
cd src
```

### Go back

```bash
cd ..
```

### Create folder

```bash
mkdir folder_name
```

### Create file

```bash
touch filename.py
```

Examples:

```bash
touch producer.py
touch consumer.py
```

### Clear terminal

```bash
clear
```

---

# 🔴 4. Git — VERY IMPORTANT

Your teacher specifically said:

> commit every change in order to open the next step.

So this is one of the most important sections.

## Check status

```bash
git status
```

This tells you what changed.

---

## Add a specific file

```bash
git add filename.py
```

Example:

```bash
git add producer.py
```

---

## Add everything

```bash
git add .
```

This is probably what you'll use most.

---

## Commit

```bash
git commit -m "Complete Kafka producer"
```

The message can be anything meaningful.

Examples:

```bash
git commit -m "Complete anomaly detection"
```

```bash
git commit -m "Add Kafka consumer"
```

```bash
git commit -m "Complete Airflow DAG"
```

---

## Push

```bash
git push
```

This sends your local commit to GitHub.

---

## Pull

```bash
git pull
```

This gets the latest changes from GitHub.

---

# 🔴 Your most important Git sequence

Memorize this:

```bash
git status
git add .
git commit -m "Complete task"
git push
```

Think:

```text
Check
 ↓
Add
 ↓
Commit
 ↓
Push
```

---

# 5. Branch Commands

Your teacher specifically mentioned branches.

### See branches

```bash
git branch
```

### Create a branch

```bash
git branch feature-name
```

Example:

```bash
git branch kafka-consumer
```

### Switch branch

```bash
git checkout kafka-consumer
```

Modern Git also supports:

```bash
git switch kafka-consumer
```

### Create AND switch

Very useful:

```bash
git checkout -b kafka-consumer
```

or:

```bash
git switch -c kafka-consumer
```

### Merge

Suppose you're on `main` and want to merge `kafka-consumer`:

```bash
git switch main
```

then:

```bash
git merge kafka-consumer
```

---

# 6. Pull Request

You probably **won't create a Pull Request using a terminal command**.

The normal process is:

```text
Create branch
      ↓
Make changes
      ↓
git add .
      ↓
git commit
      ↓
git push
      ↓
GitHub
      ↓
Create Pull Request
```

On GitHub, you'll generally see:

**Compare & pull request**

or:

**Pull requests → New pull request**

### Understand the difference

```text
Commit
= save changes locally in Git history

Push
= upload your commits to GitHub

Pull
= download latest changes from GitHub

Branch
= separate line of development

Merge
= combine branches

Pull Request
= request to merge your branch into another branch
```

---

# 7. Python Commands You'll Actually Use in Q1

The sample Q1 requires statistics, threshold anomaly detection, printing anomalies and graphs. :chatgpt-content-reference{index="3"}

Know these **Python statements**:

### Import Pandas

```python
import pandas as pd
```

### Import Matplotlib

```python
import matplotlib.pyplot as plt
```

### DataFrame

```python
df = pd.DataFrame(data)
```

### Number of records

```python
len(df)
```

### Statistics

```python
df.describe()
```

### Average

```python
df["CPU"].mean()
```

### Maximum

```python
df["CPU"].max()
```

### Minimum

```python
df["CPU"].min()
```

### Filter anomaly

```python
anomalies = df[df["CPU"] > 80]
```

### Number of anomalies

```python
len(anomalies)
```

---

# 8. Matplotlib Commands

Know:

```python
plt.plot(...)
```

```python
plt.xlabel("Timestamp")
```

```python
plt.ylabel("CPU Usage")
```

```python
plt.title("CPU Usage")
```

```python
plt.show()
```

Basic pattern:

```python
plt.plot(df["Timestamp"], df["CPU"])
plt.xlabel("Timestamp")
plt.ylabel("CPU Usage")
plt.title("CPU Usage Over Time")
plt.show()
```

---

# 🔴 9. Kafka Commands

This part needs a distinction.

Your **teacher's sample code is Python code**, not terminal commands.

The important Python Kafka commands/functions are:

### Topic

```python
KafkaAdminClient(...)
```

```python
NewTopic(...)
```

```python
admin.create_topics(...)
```

```python
admin.close()
```

Your sample specifically requires the topic:

```text
server_metrics
```

with one partition and replication factor 1. :chatgpt-content-reference{index="4"}

---

# 10. Kafka Producer

Know:

```python
KafkaProducer(...)
```

```python
producer.send(...)
```

```python
producer.flush()
```

```python
producer.close()
```

Your teacher's exact important pattern is:

```python
producer.send(
    "server_metrics",
    value=message
)
```

Meaning:

> Send `message` to the `server_metrics` topic.

---

# 11. Kafka Consumer

Know:

```python
KafkaConsumer(...)
```

Then:

```python
for message in consumer:
```

Then:

```python
data = message.value
```

Then:

```python
server = data["server_id"]
cpu = data["cpu_usage"]
memory = data["memory_usage"]
```

Then:

```python
if cpu > 80:
    print("ALERT: High CPU detected on", server)
```

This directly corresponds to the sample Q3. :chatgpt-content-reference{index="5"}

---

# 12. Kafka JSON Commands

Know:

```python
import json
```

Producer:

```python
json.dumps(x)
```

Consumer:

```python
json.loads(...)
```

Your teacher's producer uses:

```python
json.dumps(x).encode("utf-8")
```

Consumer uses:

```python
json.loads(value.decode("utf-8"))
```

You don't need to become a JSON expert. Just understand:

```text
Python dictionary
      ↓
json.dumps()
      ↓
JSON
      ↓
Kafka
```

and reverse:

```text
Kafka
 ↓
JSON bytes
 ↓
decode()
 ↓
json.loads()
 ↓
Python dictionary
```

---

# 13. Kafka Terminal Commands — Know Conceptually

Depending on the exam environment, Kafka may provide commands such as:

### Start Kafka

```bash
kafka-server-start.sh ...
```

### Create topic

```bash
kafka-topics.sh ...
```

### List topics

```bash
kafka-topics.sh --list ...
```

### Describe topic

```bash
kafka-topics.sh --describe ...
```

### Console producer

```bash
kafka-console-producer.sh ...
```

### Console consumer

```bash
kafka-console-consumer.sh ...
```

**But don't spend your time memorizing complicated flags.**

Your provided sample specifically focuses on using **Python Kafka producer/consumer code**, and your teacher said no setup is needed. :chatgpt-content-reference{index="6"}

---

# 🔴 14. Airflow

For Airflow, the most important things aren't terminal commands. They're the Python objects:

```python
DAG(...)
```

```python
PythonOperator(...)
```

and:

```python
task1 >> task2
```

Your sample requires four tasks:

```text
collect_metrics
        ↓
process_metrics
        ↓
detect_anomaly
        ↓
generate_report
```

:chatgpt-content-reference{index="7"}

---

# 15. Airflow Terminal Commands

Know these basic commands conceptually:

### Check Airflow

```bash
airflow version
```

### List DAGs

```bash
airflow dags list
```

### Check a DAG

```bash
airflow dags list
```

### Test a task

Depending on the Airflow version/environment, task-testing commands can differ, so **don't memorize random commands from older tutorials**.

Your exam sample primarily tests the DAG code, `PythonOperator`, and dependencies. :chatgpt-content-reference{index="8"}

---

# 16. Airflow Code You MUST Know

### DAG

```python
from airflow import DAG
```

### PythonOperator

```python
from airflow.operators.python import PythonOperator
```

### Function

```python
def collect_metrics():
    print("Collecting metrics")
```

### Task

```python
collect_task = PythonOperator(
    task_id="collect_metrics",
    python_callable=collect_metrics
)
```

### Dependency

```python
collect_task >> process_task
```

### Full chain

```python
collect_task >> process_task >> detect_task >> report_task
```

---

# 🔴 17. The AIOps Logic

These aren't terminal commands, but **memorize these patterns**.

### Threshold

```python
if cpu > 80:
```

### Alert

```python
print("ALERT: High CPU detected")
```

### Counter

```python
anomaly_count = 0
```

Then:

```python
anomaly_count += 1
```

Then:

```python
print("Total anomalies detected:", anomaly_count)
```

This is exactly what the integrated sample question asks you to implement. :chatgpt-content-reference{index="9"}

---

# ⭐ Your Ultimate Cheat Sheet

If you have only **15 minutes before the exam**, revise this:

### Environment

```bash
python --version
python -m pip install package
source .venv/bin/activate
python file.py
```

### Files

```bash
pwd
ls
cd folder
cd ..
touch file.py
mkdir folder
```

### Git

```bash
git status
git add .
git commit -m "message"
git push
git pull
git branch
git switch branch
git merge branch
```

### Python

```python
import pandas as pd
import matplotlib.pyplot as plt

df = pd.DataFrame(data)

df.describe()

df["CPU"].mean()
df["CPU"].max()
df["CPU"].min()

anomalies = df[df["CPU"] > 80]

plt.plot(...)
plt.show()
```

### Kafka

```python
KafkaAdminClient(...)
NewTopic(...)
admin.create_topics(...)
```

```python
KafkaProducer(...)
producer.send(...)
producer.flush()
producer.close()
```

```python
KafkaConsumer(...)

for message in consumer:
    data = message.value
```

```python
json.dumps(...)
json.loads(...)
```

### AIOps

```python
if cpu > 80:
    print("ALERT")
```

```python
anomaly_count += 1
```

### Airflow

```python
DAG(...)
PythonOperator(...)
```

```python
task1 >> task2 >> task3 >> task4
```

---

## 🔥 The 10 things I would absolutely memorize tonight

If you're tired and need to prioritize:

**1.**

```bash
source .venv/bin/activate
```

**2.**

```bash
python -m pip install package
```

**3.**

```bash
python filename.py
```

**4.**

```bash
git add .
git commit -m "message"
git push
```

**5.**

```python
anomalies = df[df["CPU"] > 80]
```

**6.**

```python
producer.send("server_metrics", value=message)
```

**7.**

```python
for message in consumer:
    data = message.value
```

**8.**

```python
if cpu > 80:
    print("ALERT: High CPU detected")
```

**9.**

```python
anomaly_count += 1
```

**10.**

```python
collect_task >> process_task >> detect_task >> report_task
```

If you know those **10 patterns + the meaning behind them**, you have the core practical mechanics represented by your sample paper.

## 🔥 Git Commands You Actually Need for the Exam

### 1. Check current status
```bash
git status
```

### 2. Add changes
```bash
git add .
```
Or specific file:
```bash
git add filename.py
```

### 3. Commit changes
```bash
git commit -m "Complete task"
```

### 4. Push to GitHub
```bash
git push
```

### 5. Pull latest changes
```bash
git pull
```

### 6. See commit history
```bash
git log --oneline
```

---

## 🌿 Branch Commands

### See branches
```bash
git branch
```

### Create + switch to new branch
```bash
git switch -c feature-name
```

### Switch branch
```bash
git switch main
```

### Merge a branch
First go to the branch receiving changes:
```bash
git switch main
```

Then:
```bash
git merge feature-name
```

### Delete branch
```bash
git branch -d feature-name
```

---

## 🔄 Most Important Exam Workflow

After completing **every task**:

```bash
git status
git add .
git commit -m "Complete Q1"
git push
```

Then start the next task.

### Example

```bash
# Finish Q1
git add q1_anomaly.py
git commit -m "Complete Q1 anomaly detection"
git push

# Work on Q2
git add topic.py producer.py
git commit -m "Add Kafka topic and producer"
git push
```

### 🧠 Remember this sequence

**ADD → COMMIT → PUSH**

```text
git add .
     ↓
git commit -m "message"
     ↓
git push
```

For your exam, these are the **most important 10**:

```bash
git status
git add .
git add filename.py
git commit -m "message"
git push
git pull
git branch
git switch main
git switch -c branch-name
git merge branch-name
```
