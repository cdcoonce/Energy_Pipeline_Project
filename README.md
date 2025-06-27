# Energy Analytics Pipeline with Dagster

## Project Overview

This project demonstrates a modern energy analytics pipeline designed using Dagster's software-defined assets. The pipeline ingests power trend data, performs analytical transformations using dbt and DuckDB, and applies orchestration and observability practices using Dagster.

A custom Dagster sensor monitors asset materialization events and sends automated email notifications when the `power_trends_by_day` asset is updated. This showcases an event-driven architecture for data monitoring and communication.

---

## Key Technologies

| Tool      | Purpose                                                  |
|-----------|----------------------------------------------------------|
| Dagster   | Data orchestrator and job scheduler using software-defined assets |
| dbt       | SQL-based data transformations and testing               |
| DuckDB    | Embedded analytical database for efficient local queries |
| Polars    | Fast, memory-efficient DataFrame library                 |
| Python    | Used for scripting, sensors, and utility functions       |
| SMTP      | Email notifications triggered by asset materialization   |

---

## Dagster Architecture

- **Software-Defined Assets**: Dagster defines assets as first-class objects. In this project, each dbt model is wrapped as a Dagster asset using `load_assets_from_dbt_project`.

- **Job Orchestration**: Asset jobs are created from asset definitions and executed using Dagster’s scheduling or manually via the Dagster UI.

- **Asset Sensor**: A Dagster `AssetSensorDefinition` listens for the materialization of the `power_trends_by_day` asset and triggers an email notification using a custom utility function.

- **No traditional DAGs**: Unlike older schedulers (e.g., Airflow), the pipeline is not hard-coded as a DAG. Dagster infers dependencies from asset relationships.

---

## Pipeline Workflow

1. **Ingest**: Power trend data is stored as a csv file.
2. **Transform**: dbt models (`power_trends_by_day`) transform the data within DuckDB.
3. **Orchestrate**: Dagster assets trigger execution of dbt models.
4. **Monitor**: Dagster asset sensor detects materialization and sends a notification via email.
5. **Analyze**: Polars is used to verify and visualize trends as DataFrames.

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/energy-pipeline-project.git
cd energy-pipeline-project
```

### 2. Set Up Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Email Notifications

Create a `.env` file in the root directory with the following format:

```
EMAIL_ADDRESS=your_email@example.com
EMAIL_PASSWORD=your_generated_app_password
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
```

> **Note**: You must enable 2FA and use an app password for Gmail or your provider.

### 4. Launch Dagster Development UI

```bash
dagster dev -m dagster_project
```

Access the pipeline UI at: [http://localhost:3000](http://localhost:3000)

---

## Project Structure

```
energy-pipeline-project/
│
├── dagster_project/
│   ├── assets.py                # Dagster asset definitions (including dbt assets)
│   ├── jobs.py                  # Asset jobs
│   ├── sensors.py               # Asset sensor for email alerts
│   ├── utils/email_utils.py     # Email utility
│   └── definitions.py           # Assembles all definitions
│
├── dbt/energy_pipeline_project/
│   ├── models/                  # dbt models
│   └── dbt_project.yml
│
├── data/
│   └── power_trends.csv     # Sample dataset
│
├── .env
├── requirements.txt
└── README.md
```

---

## Sample Output

- ✅ `power_trends_by_day` materialized via dbt + Dagster
- 📧 Email sent upon sensor trigger:

  ```
  Subject: New Trends Data Materialized
  Body: The asset 'power_trends_by_day' was just materialized.
  ```

---

## Potential Enhancements

- Add Slack or webhook notification support
- Integrate with cloud storage and external APIs
- Deploy to cloud orchestrators (e.g., Dagster Cloud or Kubernetes)
- Add CI/CD workflows for dbt + Dagster

---

## License

This project is open-sourced under the MIT License.