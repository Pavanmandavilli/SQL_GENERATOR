import streamlit as st
import os
from huggingface_hub import InferenceClient
from functools import lru_cache
import time


class SQLQueryGenerator:
    def __init__(self, api_key, model="mistralai/Mistral-7B-Instruct-v0.3"):
        self.client = InferenceClient(token=api_key)
        self.model = model

    @lru_cache(maxsize=10)
    def generate_sql(self, schema, condition):
        prompt = f"""
        You are an expert SQL query generator.
        Your task is to generate a valid and optimized SQL query based on the given table schema and condition.

        Table Schema:
        {schema}

        Condition:
        {condition}

        Ensure the query is:
        - Syntactically correct
        - Optimized for performance
        - Uses proper SQL syntax

        Provide only the SQL query as output.
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

st.set_page_config(page_title="SQL Query Generator")
api_key = st.text_input("Enter your Hugging Face API Key", type="password")


if not api_key:
    st.warning("Please enter your Hugging Face API key to continue.")
    st.stop()

st.success("API Key Added! Now you can generate SQL queries.")

st.markdown("<h1 style='color: red;'>SQL Query Generator</h1>", unsafe_allow_html=True)
st.text("Generate SQL queries from table schema and conditions")

# Input fields
schema = st.text_area("Enter the Table Schema", height=150)
condition = st.text_area("Enter the Required Condition", height=100)

if st.button("Generate SQL Query"):
    if schema and condition:
        with st.spinner("Generating SQL Query..."):
            sql_generator = SQLQueryGenerator(api_key)
            sql_query = sql_generator.generate_sql(schema, condition)
        st.subheader("Generated SQL Query")
        st.code(sql_query, language="sql")
    else:
        st.warning("Please enter both schema and condition.")
