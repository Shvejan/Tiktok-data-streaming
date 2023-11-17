from flask import Flask, render_template, request
import duckdb


app = Flask(__name__)


def connect_duckdb():
    con = duckdb.connect(database=":memory:", read_only=False)
    con.sql("install 'httpfs';")
    con.sql("load 'httpfs';")
    con.sql("SET s3_url_style='path';")
    con.sql(" SET s3_endpoint='minio:9000'")
    con.sql("SET s3_use_ssl=false;")
    con.sql("SET s3_access_key_id='minioaccesskey';")
    con.sql("SET s3_secret_access_key='miniosecretkey';")
    return con


conn = connect_duckdb()


@app.route("/", methods=["GET", "POST"])
def index():
    user_query = ""
    results = []
    error_message = ""
    row_count = 0  # Initialize row count

    if request.method == "POST":
        user_query = request.form["search"]

        # Replace 'TABLE' in the user query with the actual parquet file path
        safe_query = user_query.replace(
            "TABLE", "read_parquet('s3://query-bucket/db.parquet')"
        )

        # Construct the COUNT query
        count_query = (
            "SELECT COUNT(*) FROM " + "read_parquet('s3://query-bucket/db.parquet')"
        )

        try:
            # Execute the user query
            results = conn.execute(safe_query).fetchall()
            columns = [desc[0] for desc in conn.description]
            results = [dict(zip(columns, row)) for row in results]

            # Execute the COUNT query
            row_count_result = conn.execute(count_query).fetchone()
            row_count = row_count_result[0] if row_count_result else 0
        except Exception as e:
            error_message = str(e)
            results = []

    return render_template(
        "index.html",
        results=results,
        user_query=user_query,
        row_count=row_count,
        error_message=error_message,
    )


if __name__ == "__main__":
    app.run(debug=True, port=3000, host="0.0.0.0")
