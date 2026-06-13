# streaming-06-scenarios

[![API Reference](https://img.shields.io/badge/API--Utils-datafun--streaming-purple)](https://denisecase.github.io/datafun-streaming/api/)
[![Workflow Guide](https://img.shields.io/badge/Pro--Guide-pro--analytics--02-green)](https://denisecase.github.io/pro-analytics-02/workflow-b-apply-example-project/)
[![Python 3.14](https://img.shields.io/badge/python-3.14%2B-blue?logo=python)](./pyproject.toml)
[![MIT](https://img.shields.io/badge/license-see%20LICENSE-yellow.svg)](./LICENSE)

> Streaming data analytics: complete pipeline.

This project demonstrates a complete streaming analytics pipeline using Kafka,
DuckDB, and Python. Sales transactions are streamed through Kafka, processed by
a consumer, enriched with derived fields, visualized, and stored for analysis.

## This Project

This project brings the full streaming analytics workflow together.

The project uses Kafka to move sales messages from a producer to a consumer.
The producer sends validated sales messages to a Kafka topic.
The consumer reads each message, validates required fields, computes derived values,
updates a live chart, writes processed records to CSV, and stores results in DuckDB.

This project combines several streaming analytics concepts:

- producing messages
- consuming messages
- validating message structure
- computing derived fields
- visualizing the stream
- storing processed data

The goal is to see how the parts work together in one complete scenario.

For my custom project, I extended the pipeline by classifying transactions as
low, medium, or high value based on total sale amount. This allows the consumer
to generate additional analytics that provide insight into customer spending
patterns and sales performance.

## Dataset

This project streams records from `data/sales.csv`, which contains
sales transaction records including order information, product details,
customer information, pricing, and payment data.

The project also uses `regions.csv`, `products.csv`,
`currencies.csv`, and `discount_codes.csv` as static reference tables
for validation and enrichment.

## Kafka Topic

Topic: `streaming-06-scenarios-kjleopold`

Message Key: `region_id`

Using `region_id` as the message key groups sales messages by region and
helps organize the streaming data as it moves through Kafka.

## Consumer Processing

The consumer receives sales transaction messages from Kafka and
processes each record by:

- validating required fields
- calculating derived fields including subtotal, tax_amount, total, and customer_value
- classifying transactions by customer_value
- updating a live sales chart
- writing processed records to CSV
- storing records in DuckDB
- generating summary analytics by region and customer value category

## Custom Modifications

These modifications extend the original sales pipeline by classifying
transactions into customer value categories and generating additional analytics
from those classifications.

For Phase 4, I added a derived field called `customer_value` that classifies
sales as low, medium, or high based on the total sale amount.

For Phase 5, I stored the new field in DuckDB and added a summary query that
counts transactions by customer value category.

The customer_value field classifies transactions as:

- low (< $100)
- medium ($100-$249.99)
- high ($250+)

Custom files include:

- [derived_fields_kjleopold.py](src/streaming/data_engineering/derived_fields_kjleopold.py)
  - Adds the customer_value derived field.

- [storage_kjleopold.py](src/streaming/storage/storage_kjleopold.py)
  - Adds DuckDB analytics by customer value category.

- [kafka_consumer_kjleopold.py](src/streaming/kafka_consumer_kjleopold.py)
  - Processes messages and logs customer value summaries.

## Project Structure

- **data/** - input data and generated output files
- **docs/** - the project narrative and documentation
- **src/streaming/** - producer, consumer, and supporting code
- **pyproject.toml** - project configuration
- **zensical.toml** - project documentation configuration

## Success

After completing the setup steps below, you will have a working Kafka
streaming pipeline running locally.

Use four named terminals:

1. **kafka** - keep the Kafka message broker running
2. **topics** - create, list, or reset Kafka topics
3. **producer** - run the project and producer
4. **consumer** - run the consumer

After the producer and consumer run successfully, you should see:

```shell
========================
Consumer executed successfully!
========================
```

A new file `project.log` will appear in the root project folder
and processed data will appear in data/output/.

You should also see sales records stored in DuckDB, a generated sales chart,
and analytics summarizing transactions by customer value category (low,
medium, and high).

## Sample Output

![Sales Chart](data/output/sales_chart_kjleopold.png)

## Output Artifacts

Expected output files include:

- consumed_sales_kjleopold.csv
- sales_kjleopold.duckdb
- sales_chart_kjleopold.png
- project.log

## Setup and Run Instructions

The instructions below can be used to set up and run the project.

**Important:** If you are new to Kafka or this project setup,
review the linked Kafka documentation before running the project.

<details>
<summary>Show detailed setup and run instructions</summary>

### In a machine terminal (open in your `Repos` folder)

After you get a copy of this repo in your own GitHub account,
open a machine terminal in your `Repos` folder:

```bash
git clone https://github.com/kjleopold/streaming-06-scenarios

cd streaming-06-scenarios
code .
```

### Set Up Virtual Environment

Open a VS Code terminal in your project folder.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# set up virtual environment
uv self update
uv python pin 3.14
uv sync --extra dev --extra docs --upgrade

# set up pre-commit hooks (not needed for peer review)
uvx pre-commit install
uvx pre-commit run --all-files
```

### Activate VS Code

- Open the Command Palette (menu: View/Command Palette, or `Ctrl+Shift+P`)
- Type and choose: `Python: Select Interpreter`
- Choose the interpreter inside this project's `.venv` folder (usually .\.venv\Scripts\python.exe)
- Open the Command Palette again (same as before)
- Type or choose: `Developer: Reload Window`

### In VS Code Terminal 1: Start Kafka (kafka)

For full instructions see
[**start kafka**](https://denisecase.github.io/pro-analytics-02/kafka/start-kafka/).

If any command fails,
repeat the steps at
[**install kafka**](https://denisecase.github.io/pro-analytics-02/kafka/install-kafka/)
until starting up is reliable.

Open a new VS Code terminal. Rename it `kafka`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

Step 1. Verify Java and PATH

```bash
echo "$JAVA_HOME"

"$JAVA_HOME/bin/java" --version
```

Step 2. Rebuild ClusterID (as needed)

```bash
cd ~/kafka

rm -rf /tmp/kraft-combined-logs

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"

echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

Step 3. Start kafka server (keep running)

```bash
cd ~/kafka

bin/kafka-server-start.sh config/server.properties
```

### In VS Code Terminal 2: Verify Kafka and Create Topic (topics)

The Kafka admin utility verifies the Kafka connection and manages the
project topic. It can create the topic, delete it, or recreate it from
scratch. Instructions for both the admin utility and manual topic
creation are included below.

To use the admin utility, if running Windows, use a **PowerShell** terminal.

Run:

```shell
# create topic
uv run python -m streaming.kafka_admin_kjleopold

# delete and recreate topic
uv run python -m streaming.kafka_admin_kjleopold --recreate

# delete topic
uv run python -m streaming.kafka_admin_kjleopold --delete
```

To manually create the topic instead of running the admin utility,
see the instructions below.

The topic name must match the name defined in your
`.env` file (copy `.env.example` to `.env`).

Open another VS Code terminal. Rename it `topics`.
If running Windows, specify the terminal type as **wsl** or
type `wsl`.
Run the commands one at a time.

```bash
cd ~/kafka

bin/kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --topic streaming-06-scenarios-kjleopold
```

### In VS Code Terminal 3: Run Project and Producer (producer)

Open another VS Code terminal. Rename it `producer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.

```shell
# reset uv cache only if/when you start getting strange dependency errors
# uv cache clean

# run the producer
clear
uv run python -m streaming.kafka_producer_kjleopold

# do chores (not needed for peer review)
uv run ruff format .
uv run ruff check . --fix
uv run python -m pyright
uv run python -m pytest
uv run python -m zensical build

# save progress (not needed for peer review)
git add -A
git commit -m "update"
git push -u origin main
```

### In VS Code Terminal 4: Run Consumer (consumer)

Open another VS Code terminal. Rename it `consumer`.
If running Windows, use **PowerShell**.
Run the commands one at a time.
Clear the terminal, then start the consumer.

```shell
clear
uv run python -m streaming.kafka_consumer_kjleopold
```

### Save Progress

```shell
# not needed for peer review
git add -A
git commit -m "add a meaningful comment"
git push -u origin main
```

</details>
