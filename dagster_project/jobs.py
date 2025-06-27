from dagster import job, op

@op
def notify_op(context):
    context.log.info("🚨 power_trends_by_day was just updated!")

@job
def notify_on_trends_job():
    notify_op()