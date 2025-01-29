from flask import Flask, request, jsonify, render_template_string
import os
import time
from huggingface_hub import InferenceClient
from functools import lru_cache
import subprocess

app = Flask(__name__)

# Store the latest SQL query
latest_sql_query = ""


class SQLQueryGenerator:
    def __init__(self, api_key, model="mistralai/Mistral-7B-Instruct-v0.3"):
        self.client = InferenceClient(token=api_key)
        self.model = model

    @lru_cache(maxsize=10)
    def generate_sql(self, schema, condition):
        prompt = f"""
                You are an expert BigQuery query generator.
                Your task is to generate a valid and optimized BigQuery SQL query based on the given table schema and condition.

                Table Schema:
                {schema}

                Condition:
                {condition}

                projectid:
                {pid}

                datasetid:
                {did}

                Ensure the query is:
                - Syntactically correct
                - Optimized for performance
                - Uses proper BigQuery SQL syntax
                - Includes the project ID and dataset ID in the table reference

                Provide only the BigQuery SQL query as output. Do not include any explanations.

                Example format:
                SELECT column_name 
                FROM `project_id.dataset_id.table_name` 
                WHERE condition;
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
        api_key = request.form.get("api_key")
        schema = request.form.get("schema")
        condition = request.form.get("condition")

        if not api_key or not schema or not condition:
            return render_template_string(HTML_TEMPLATE, error="All fields are required!")

        sql_generator = SQLQueryGenerator(api_key)
        latest_sql_query = sql_generator.generate_sql(schema, condition)

        return render_template_string(HTML_TEMPLATE, query=latest_sql_query)

    return render_template_string(HTML_TEMPLATE)


# API Endpoint to Generate SQL Query
@app.route("/generate_sql", methods=["POST"])
def generate_sql():
    global latest_sql_query
    data = request.json

    api_key = data.get("api_key")
    # schema = data.get("schema")
    pid = 'dev-kapture'
    did = 'devDataset'
    schema = '''
            TABLE %s.%s.assigned_to_resolve_report (\n" +
                        "  id INT64 NOT NULL,\n" +
                        "  cm_id INT64,\n" +
                        "  ticket_id STRING(256),\n" +
                        "  ticket_status STRING(1),\n" +
                        "  agent_id INT64,\n" +
                        "  reopen_by_agent_id INT64,\n" +
                        "  created_date TIMESTAMP,\n" +
                        "  assigned_date TIMESTAMP,\n" +
                        "  first_replied_date TIMESTAMP,\n" +
                        "  disposed_date TIMESTAMP,\n" +
                        "  disposition_type STRING(5),\n" +
                        "  disposition_folder_id INT64,\n" +
                        "  agent_replied_count INT64,\n" +
                        "  customer_replied_count INT64,\n" +
                        "  dispose_remark STRING,\n" +
                        "  source STRING(1),\n" +
                        "  is_out_of_sla BOOL,\n" +
                        "  ticket_category STRING(1),\n" +
                        "  type_reference STRING(100),\n" +
                        "  task_id INT64,\n" +
                        "  dispose_id INT64,\n" +
                        "  current_status STRING(1),\n" +
                        "  is_created BOOL,\n" +
                        "  last_reply_time TIMESTAMP,\n" +
                        "  first_customer_replied_time TIMESTAMP,\n" +
                        "  last_customer_replied_time TIMESTAMP,\n" +
                        "  last_reply_by INT64,\n" +
                        "  landing_folder_id INT64,\n" +
                        "  call_back_time TIMESTAMP,\n" +
                        "  create_reason STRING(50),\n" +
                        "  landing_queue STRING(50),\n" +
                        "  last_queue STRING(50),\n" +
                        "  is_first_assign BOOL,\n" +
                        "  first_assign_time TIMESTAMP,\n" +
                        "  is_resolve_without_dispose BOOL,\n" +
                        "  agent_remark STRING,\n" +
                        "  first_replied_by INT64,\n" +
                        "  ticket_create_date TIMESTAMP,\n" +
                        "  average_time FLOAT64,\n" +
                        "  reopen_count INT64,\n" +
                        "  email STRING(100),\n" +
                        "  phone STRING(100),\n" +
                        "  is_sub_task BOOL,\n" +
                        "  is_chat_bot BOOL\n" +
                        ") \n" +
    '''
    condition = data.get("condition")

    if not api_key or not schema or not condition:
        return jsonify({"error": "Missing required parameters"}), 400

    sql_generator = SQLQueryGenerator(api_key)
    latest_sql_query = sql_generator.generate_sql(schema, condition)
    return str(latest_sql_query)
    # return jsonify({"sql_query": latest_sql_query})


# API Endpoint to Get the Last Generated SQL Query
@app.route("/get_sql_query", methods=["GET"])
def get_sql_query():
    if latest_sql_query:
        # return jsonify({"sql_query": latest_sql_query})
        return str(latest_sql_query)
    # return jsonify({"error": "No SQL query has been generated yet"}), 404


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
        <label>Hugging Face API Key:</label>
        <input type="password" name="api_key" required>

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

