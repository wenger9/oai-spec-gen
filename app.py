import streamlit as st
import yaml

st.set_page_config(page_title="DBX Connector Generator", layout="wide")

st.title("DBX Connector Generator")

# User inputs
databricks_instance = st.text_input("Databricks Instance", "adb-################.##")
warehouse_id = st.text_input("Warehouse ID", "abcdefghijklmnop")
db_name = st.text_input("Database Name", "db_name")
table_names_input = st.text_area("Enter table names, separated by commas or new lines",
                                 "jordan, jackson, tyson")

# Parse table names and remove whitespace
table_names = [table.strip() for table in table_names_input.split(",") if table.strip()]
tables_description = ', '.join(table_names)

# Button to generate the YML file
if st.button("Generate Config File"):
    schemas = {}
    schema_refs = []
    for table_name in table_names:
        schema_name = f"{table_name}QueryResult"
        schemas[schema_name] = {
            "type": "object",
            "properties": {
                "Column1": {"type": "string", "maxLength": 20},
                "Column2": {"type": "integer", "maxLength": 10},
                "Column3": {"type": "float", "maxLength": 15},
                "Column4": {"type": "boolean"},
                "Column5": {"type": "date"}
            }
        }
        schema_refs.append({"$ref": f"#/components/schemas/{schema_name}"})

    config_data = {
        "openapi": "3.0.0",
        "info": {
            "title": "Databricks SQL API Integration",
            "description": f"Execute SQL queries on Databricks and retrieve results for specific tables like {tables_description}.",
            "version": "1.0.5"
        },
        "servers": [
            {
                "url": f"https://{databricks_instance}.azuredatabricks.net/api/2.0",
                "description": "Databricks API server."
            }
        ],
        "security": [
            {"DatabricksToken": []}
        ],
        "components": {
            "securitySchemes": {
                "DatabricksToken": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "Authorization",
                    "description": "Personal access token for Databricks API."
                }
            },
            "schemas": {
                **schemas,
                "SqlStatementRequest": {
                    "type": "object",
                    "required": ["statement", "warehouse_id"],
                    "properties": {
                        "statement": {
                            "type": "string",
                            "description": "SQL query to execute.",
                            "example": f"SELECT * FROM {db_name}.{table_names[0]} LIMIT 10" if table_names else ""
                        },
                        "warehouse_id": {
                            "type": "string",
                            "description": "ID of the warehouse where the query will be executed.",
                            "example": warehouse_id
                        },
                        "row_limit": {
                            "type": "integer",
                            "format": "int64",
                            "description": "Limits the number of rows returned by the query."
                        },
                        "disposition": {
                            "type": "string",
                            "enum": ["INLINE", "EXTERNAL_LINKS"],
                            "default": "INLINE",
                            "description": "The fetch disposition for results."
                        }
                    }
                }
            }
        },
        "paths": {
            "/sql/statements": {
                "post": {
                    "operationId": "executeSqlStatement",
                    "summary": f"Execute a SQL query on the Databricks SQL Warehouse for {tables_description}",
                    "security": [{"DatabricksToken": []}],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/SqlStatementRequest"}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "Query executed successfully, results are returned inline.",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "oneOf": schema_refs
                                    }
                                }
                            }
                        },
                        "400": {"description": "Bad request, such as a malformed SQL statement or invalid parameters."},
                        "401": {"description": "Unauthorized, token missing or invalid."},
                        "403": {"description": "Forbidden, token doesn't have the necessary permissions."},
                        "404": {"description": "Not Found, endpoint not found or warehouse_id incorrect."},
                        "500": {"description": "Internal Server Error, something went wrong on the server side."}
                    }
                }
            }
        }
    }

    # Convert Python dictionary to YAML
    yaml_content = yaml.dump(config_data, sort_keys=False, default_flow_style=False)

    # Display and download YAML
    st.text_area("Generated YAML", yaml_content, height=300)
    st.download_button("Download YAML File", yaml_content, file_name="generated_config.yml")
