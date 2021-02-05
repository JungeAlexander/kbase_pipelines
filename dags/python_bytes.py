# At 17:30 daily
# (Task 1 - fetch)
# Fetch RSS feed
#
# (Task 2 - filter)
# Look T (default: one week) back to fetch all episodes
# Check for all episodes if already in DB based on ID
# If not in DB, check if transcript is there
#
# (Task 3 - store)
# If transcript there, (parse everything and add to DB
# Remember set: extra: { 'transcipt_added' : True }
# Run using backfilling for past dates?

# Questions
# Xcom in taskflow -- too much data, e.g. RSS feed
# HTTPOperator together with TaskFlow? https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/operators/http/index.html#airflow.providers.http.operators.http.SimpleHttpOperator
