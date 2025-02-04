import os

from flask import Flask, request, jsonify, render_template_string
import time
from huggingface_hub import InferenceClient
from functools import lru_cache
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)

# # Hardcoded API Key & Schema

API_KEY = os.getenv("MISTRAL_API_KEY")
SCHEMA = """TABLE project_id.dataset_id.assigned_to_resolve_report (
    id INT64 NOT NULL,
    cm_id INT64,
    ticket_id STRING(256),
    ticket_status STRING(1),
    agent_id INT64,
    reopen_by_agent_id INT64,
    created_date TIMESTAMP,
    assigned_date TIMESTAMP,
    first_replied_date TIMESTAMP,
    disposed_date TIMESTAMP,
    disposition_type STRING(5),
    disposition_folder_id INT64,
    agent_replied_count INT64,
    customer_replied_count INT64,
    dispose_remark STRING,
    source STRING(1),
    is_out_of_sla BOOL,
    ticket_category STRING(1),
    type_reference STRING(100),
    task_id INT64,
    dispose_id INT64,
    current_status STRING(1),
    is_created BOOL,
    last_reply_time TIMESTAMP,
    first_customer_replied_time TIMESTAMP,
    last_customer_replied_time TIMESTAMP,
    last_reply_by INT64,
    landing_folder_id INT64,
    call_back_time TIMESTAMP,
    create_reason STRING(50),
    landing_queue STRING(50),
    last_queue STRING(50),
    is_first_assign BOOL,
    first_assign_time TIMESTAMP,
    is_resolve_without_dispose BOOL,
    agent_remark STRING,
    first_replied_by INT64,
    ticket_create_date TIMESTAMP,
    average_time FLOAT64,
    reopen_count INT64,
    email STRING(100),
    phone STRING(100),
    is_sub_task BOOL,
    is_chat_bot BOOL
)"""

# Store the latest SQL query
latest_sql_query = ""


class SQLQueryGenerator:
    def __init__(self, api_key, model="mistralai/Mistral-7B-Instruct-v0.3"):
        self.client = InferenceClient(token=api_key)
        self.model = model

    
    @lru_cache(maxsize=10)
    def generate_sql(self, condition):

        pid = "dev-kapture"
        did = "demoDataset"       
        
        prompt = f"""
        You are an expert BigQuery query generator.
        Your task is to generate a valid and optimized BigQuery SQL query based on the given table schema and condition.And change project_id and database_id.

         ### CRITICAL TIMESTAMP VS DATE RULES:
        1. Full timestamp (when time is included):
           Input: '2025-01-22 02:34:07'
           MUST USE: WHERE date_column = TIMESTAMP('2025-01-22 02:34:07')
           
        2. ### Date-only format:
           Date only (e.g., 2025-01-22):
           MUST USE: 
           WHERE date_column **BETWEEN** TIMESTAMP('YYYY-MM-DD 00:00:00') AND TIMESTAMP('YYYY-MM-DD 23:59:59')


        ### Query Requirements:
        1. Use the specified project_id ({pid}) and dataset_id ({did})
        2. Only select columns that are specifically requested in the condition
        3. If no specific columns are mentioned, then use SELECT *
        4. Apply appropriate WHERE clauses based on the condition
        5. Use proper BigQuery SQL syntax and optimization
    
        ### Date Column Handling:
        For these columns: disposed_date, created_date, assigned_date, first_replied_date, ticket_create_date
        Timestamp format:
        - Full timestamp (e.g., 2025-01-22 02:34:07):
          WHERE date_column = TIMESTAMP('2025-01-22 02:34:07')
    
        Date-only format:
        - Date only (e.g., 2025-01-22):
          WHERE date_column BETWEEN TIMESTAMP('2025-01-22 00:00:00') AND TIMESTAMP('2025-01-22 23:59:59')
    
        Notes:
        - Always interpret `created date` as created_date
        - Use single quotes for TIMESTAMP values
        - Include full timestamp precision
    
        ### Example Queries:
        Example 1 - Specific columns with date:
        Condition: `Get ticket_id and agent_id for tickets created on 2025-01-22`
        SELECT ticket_id, agent_id
        FROM `{pid}.{did}.assigned_to_resolve_report`
        WHERE created_date BETWEEN TIMESTAMP(`2025-01-22 00:00:00`) AND TIMESTAMP(`2025-01-22 23:59:59`);
    
        Example 2 - Aggregation:
        Condition: `Count tickets by status and agent_id`
        SELECT ticket_status, agent_id, COUNT(*) as ticket_count
        FROM `{pid}.{did}.assigned_to_resolve_report`
        GROUP BY ticket_status, agent_id;
    
        Ensure the query is:
        - Syntactically correct
        - Optimized for performance
        - Uses proper BigQuery SQL syntax
        - Includes the project ID and dataset ID in the table reference
        
        
        Table Schema:
        {SCHEMA}
        
        Condition:
        {condition}
        
        project_id:
        {pid}
        
        database_id:
        {did}

        Provide only the BigQuery SQL query as output without any markdown symbols, formatting and not include any explanations.
        
        """


        messages = [{"role": "user", "content": prompt}]
        try:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=500
            )
            sql_query = completion.choices[0].message["content"]
            time.sleep(2)
            return sql_query.strip()
        except Exception as e:
            return f"Error generating SQL: {e}"


# Flask Route for Web Interface
@app.route("/", methods=["GET", "POST"])
def index():
    global latest_sql_query
    if request.method == "POST":
        condition = request.form.get("condition")

        if not condition:
            return render_template_string(HTML_TEMPLATE, error="Condition is required!")

        sql_generator = SQLQueryGenerator(API_KEY)
        latest_sql_query = sql_generator.generate_sql(condition)

        return render_template_string(HTML_TEMPLATE, query=latest_sql_query)

    return render_template_string(HTML_TEMPLATE)


# API Endpoint to Generate SQL Query
@app.route("/generate_sql", methods=["POST"])
def generate_sql():
    global latest_sql_query
    data = request.json

    condition = data.get("condition")

    if not condition:
        return jsonify({"error": "Missing required parameters"}), 400

    sql_generator = SQLQueryGenerator(API_KEY)
    latest_sql_query = sql_generator.generate_sql(condition)
    # return str(latest_sql_query)
    return jsonify({"query": latest_sql_query})


# API Endpoint to Get the Last Generated SQL Query
@app.route("/get_sql_query", methods=["GET"])
def get_sql_query():
    if latest_sql_query:
        return str(latest_sql_query)
    return jsonify({"error": "No SQL query has been generated yet"}), 404


# Simple HTML Form for User Interface
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL Query Generator</title>
    <style>
        body { font-family: Arial, sans-serif; text-align: center; margin: 50px; }
        form { display: inline-block; text-align: left; width: 400px; }
        textarea { width: 100%; height: 80px; }
        input, button { width: 100%; padding: 10px; margin-top: 10px; }
        pre { background: #f4f4f4; padding: 10px; text-align: left; white-space: pre-wrap; }
        .error { color: red; }
    </style>
</head>
<body>
    <h1 style="color: red;">SQL Query Generator</h1>
    <form method="POST">
        <label>Condition:</label>
        <textarea name="condition" required></textarea>

        <button type="submit">Generate SQL Query</button>
    </form>

    {% if error %}
        <p class="error">{{ error }}</p>
    {% endif %}

    {% if query %}
        <h2>Generated SQL Query:</h2>
        <pre>{{ query }}</pre>
    {% endif %}
</body>
</html>
"""
