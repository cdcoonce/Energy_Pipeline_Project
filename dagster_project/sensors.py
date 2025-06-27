from dagster import asset_sensor, AssetKey, DefaultSensorStatus, SensorEvaluationContext
from dagster_project.utils.email_utils import send_email

@asset_sensor(asset_key=AssetKey("power_trends_by_day"), default_status=DefaultSensorStatus.RUNNING)
def trends_sensor(context: SensorEvaluationContext, _event):
    print("Sensor triggered!")
    subject = "New Trends Data Materialized"
    body = "The asset 'power_trends_by_day' was just materialized."
    recipient = "cdcoonce@asu.edu"
    
    send_email(subject, body, recipient)

    # No job to run; just the email as a side effect
    return []