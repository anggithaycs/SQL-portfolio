/*
  This Python code defines a Flask route, /daily_recap, that handles POST requests, extracting parameters like variable_key, start_date, end_date, and status. 
It then uses these parameters to execute a function (run_store_daily_activities) that retrieves and returns store daily activity data from a database using a DatabaseManager.
*/
@app.route('/daily_recap', methods=['POST'])
@require_variable_key2
def daily_recap():
    variable_key = request.form.get('variable_key')
    db_manager = DatabaseManager(variable_key)

    start_date_key = request.form.get('start_date')
    end_date_key = request.form.get('end_date')
    status = request.form.get('status') 

    start_date, end_date = get_date_keys(start_date_key, end_date_key)
    if start_date is None or end_date is None:
        return jsonify({"error": "Invalid date format. Please use YYYY-MM-DD format."}), 400

    result = store_daily_activities.run_store_daily_activities(db_manager, start_date, end_date,status)
    return jsonify(result)

