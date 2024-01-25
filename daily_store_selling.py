/*
The provided Python script defines a function, run_store_daily_activities, which queries a database for aggregated store daily activity metrics 
within a specified date range and optional status filter. The results are formatted into a JSON response containing total orders, sales numbers, 
stores' order count, total purchases, stores' purchase count, and spending metrics.

This python is later called in Flask app.
*/

import json
from datetime import datetime, timedelta

def run_store_daily_activities(db_manager, start_date, end_date, status=None):
    # Start building the SQL query
    sql_query = """
    SELECT 
    date_key,
    SUM(orders) total_order,
    SUM(sales_numbers) sales_number,
    SUM(selling_stores) stores_order,
    SUM(purchases_made) total_purchase,
    SUM(purchasing_stores) stores_purchase,
    SUM(purchases_numbers) spending
    FROM v_store_daily_activities_new
    WHERE date_key BETWEEN %s AND DATE_ADD(%s, INTERVAL 1 DAY)
    """

    params = [start_date, end_date]

    # Add status filter to the query if status is provided
    if status:
        sql_query += " AND store_status COLLATE utf8mb4_unicode_ci = %s"
        params.append(status)

    # Add GROUP BY and ORDER BY clauses to the query
    sql_query += " GROUP BY 1 ORDER BY date_key DESC"

    # Use DatabaseManager's run_query
    result = db_manager.run_query(sql_query, tuple(params))

    if not isinstance(result, str):
        result_json = [{
            "data": {
                "date_key": str(row[0]),
                "total_order": str(row[1]) if row[1] is not None else "0",
                "sales_number": str(row[2]) if row[2] is not None else "0",
                "stores_order": str(row[3]) if row[3] is not None else "0",
                "total_purchase": str(row[4]) if row[4] is not None else "0",
                "stores_purchase": str(row[5]) if row[5] is not None else "0",
                "spending": str(row[6]) if row[6] is not None else "0"
            }
        } for row in result]
        return result_json
    else:
        return [{"error": result}]
