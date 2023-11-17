![Data Pipeline Architecture](./Data-Pipeline.jpg)

### Steps to Run the Project

1. Make sure you have Docker installed.
2. Run the following command:
   ```bash
   docker-compose up
   ```

### File and Folder Descriptions

1. scrapper.py: Web scraper built using Selenium.
2. spark-jobs/job-scheduler.py: Batch processing script.
3. kafka-consumer/consumer.py: Kafka consumer script to store batch data in MinIO.
4. flask-server/app.py: Flask server.
