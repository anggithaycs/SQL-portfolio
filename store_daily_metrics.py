/*
This Python script uses MySQL Connector to execute SQL queries for retrieving store daily metrics within a specified date range. 
The get_store_daily_metrics function formats the results into a JSON response, allowing optional filtering by PSC or store ID.
*/
import mysql.connector
import sys
from collections import OrderedDict
import json
import re
from datetime import datetime, timedelta
import logging
from functools import wraps
from db_config import get_db_config

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def run_query(db_config, sql_query, params):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute(sql_query, params)
        result = cursor.fetchall()
        conn.close()
        return result
    except Exception as e:
        return str(e)

def get_store_daily_metrics(variable_key, start_date_key, end_date_key, psc_id=None, store_id=None):

    db_config = get_db_config(variable_key)

    # Construct the SQL query with optional parameters
    sql_query = """
    SELECT * FROM v_store_daily_metrics 
    WHERE date_key BETWEEN %s AND %s
    """
    params = [start_date_key, end_date_key]

    if psc_id:
        sql_query += " AND psc_id = %s"
        params.append(psc_id)

    if store_id:
        # Assuming store_id can be in the format 'store-<id>'
        if store_id.startswith('store-'):
            store_id = store_id.split('-', 1)[1]
        sql_query += " AND store_id = %s"
        params.append(store_id)

    sql_query += " ORDER BY date_key DESC"

    # Run the query and get the result
    result = run_query(db_config, sql_query, params)

    # Format the result as JSON
    if not isinstance(result, str):
        result_json = [{
            "data": OrderedDict([
                ("date_key", str(row[0])),
                ("store_id", row[1]),
                ("psc_id", row[2]),
                ("store_name", row[3]),
                ("psc_name", row[4]),
                ("total_order", float(row[5]) if row[5] is not None else 0.0),
                ("order_volume", float(row[6]) if row[6] is not None else 0.0),
                ("total_purchase", float(row[9]) if row[9] is not None else 0.0),
                ("purchase_volume", float(row[10]) if row[10] is not None else 0.0),
                ("total_success", float(row[11]) if row[11] is not None else 0.0),
                ("is_active", row[12])
            ])
        } for row in result]
        return result_json
    else:
        return [{"data": result}]
