# At 17:30 daily
# Fetch RSS feed
# Look T (default: one week) back to fetch all episodes
# Check for all episodes if already in DB based on ID
# If not in DB, check if transcript is there
    # If transcript there, parse everything and add to DB
# Remember set: extra: { 'transcipt_added' : True }
# Run using backfilling for past dates?