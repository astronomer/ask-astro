# Monitoring and Observability for Ask-Astro

## Overview

This document outlines the monitoring and observability practices implemented for Ask-Astro. These practices are designed to ensure the system's reliability, performance, and quick detection and resolution of issues. They can be adapted for similar systems requiring robust monitoring solutions.

## Infrastructure Monitoring

### Airflow DAGs

- **Monitoring DAG**: This [DAG](../airflow/dags/monitor/monitor.py) runs every 5 minutes to check the health of all infrastructure components.
- **Ingestion DAG Monitoring**: This [DAG](../airflow/dags/monitor/monitor_ingestion_dags.py) monitors the health of ingestion processes.

- ![Airflow DAGs](./_static/images/monitoring/airflow_dags.png)

### Alerting

- **Slack Integration**: In case of any failures, alerts are sent to the designated Slack channel as DAGs run every 5 minutes. Regular status updates are also posted daily.

![slack alerts](./_static/images/monitoring/slack_alerts.png)

### Components Monitored

1. **APIs**: Back-end APIs are tested in sequence. Failure of any test results in an immediate alert.
  ![APIs swagger UI](./_static/images/monitoring/api_swagger.png)

2. **Google Firebase**: Monitoring the Firestore app for its existence and health.

3. **Weaviate Database**: Ensuring the presence of necessary classes and checking embedding counts.

4. **UI Monitoring**: Regular checks of the UI for a 200 response status.
   - UI Link: [https://ask.astronomer.io/](https://ask.astronomer.io/)

5. **Airflow Data Ingestion and Feedback DAGs**: Monitoring for completeness and errors.

6. **Open AI Integration**: Ensuring the availability and response quality of Open AI services.

## LLM Model Monitoring

Utilizing **Langsmith** or a similar AI monitoring platform for detailed insights into the LLM's performance.

![langsmith dashboard](./_static/images/monitoring/langsmith1.png)
![langsmith latency](./_static/images/monitoring/latency.png)

### Aspects Monitored

1. **Usage Statistics**: Tracking query frequency, types, and usage patterns.

2. **Response Quality**: Evaluating accuracy, relevance, and helpfulness of LLM responses.

3. **User Feedback**: Collecting and analyzing user feedback for continuous improvement.

4. **Volume Metrics**: Monitoring trace counts, call counts, and success rates.

5. **Token Analysis**: Examining patterns in the model's responses.

6. **Error Rates**: Keeping track of model error rates to maintain reliability.

7. **Latency Metrics**: Measuring response times for optimal user experience.


## Handling Failures

Detailed procedures for handling failures are should be documented and followed. These procedures should include:
1. **Alerts**: Alerts should be sent to the designated Slack channel for immediate attention.
2. **Error Logs**: Error logs should be generated and stored for future reference.
3. **Error Resolution Documentation**: The error resolution process should be documented for future reference.
4. **Rolling back Deployment**: In case of a deployment failure, the previous version should be rolled back to ensure system availability.
5. **Point of Contact for each Component**: A point of contact should be designated for each component to ensure quick resolution of issues.
6. **Update the user**: The user should be notified of the issue and the expected resolution time.


## Conclusion

This monitoring setup is crucial for maintaining the operational efficiency and reliability of Ask-Astro. It can be adapted and applied to similar systems to ensure consistent performance and quick issue resolution.
