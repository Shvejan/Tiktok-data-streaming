from flask import Flask, request
from flask_cors import CORS, cross_origin
import requests
import pandas as pd
import duckdb
import os
import datamart_profiler
from minio import Minio
from io import BytesIO

app = Flask(__name__)
client = Minio(
    "localhost:8050", access_key="devkey", secret_key="devpassword", secure=False
)


def download_and_save(id):
    filename = id.replace(".", "_").replace("-", "_") + ".parquet"
    response = requests.get("https://auctus.vida-nyu.org/api/v1/download/" + id)
    if response.status_code == 200:
        try:
            df = pd.read_csv(BytesIO(response.content))
        except:
            return False

        profiles = datamart_profiler.process_dataset(df)
        for c in profiles["columns"]:
            dtypes = []
            dtypes.append(c["structural_type"].split("/")[-1])
            for s in c["semantic_types"]:
                dtypes.append(s.split("/")[-1])
            try:
                if "DateTime" in dtypes:
                    df[c["name"]] = pd.to_datetime(df[c["name"]], format="%Y-%m-%d")
                elif "Integer" in dtypes:
                    df[c["name"]] = df[c["name"]].fillna(0).astype(float).astype(int)
                elif "Float" in dtypes:
                    df[c["name"]] = df[c["name"]].fillna(0).astype(float)
                elif "Text" in dtypes:
                    df[c["name"]] = df[c["name"]].fillna("null").astype(str)
            except:
                pass

        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer)

        # Upload the Parquet file to MinIO
        parquet_buffer.seek(0)
        client.put_object(
            bucket_name="auctus-bucket",
            object_name=filename,
            data=parquet_buffer,
            length=parquet_buffer.getbuffer().nbytes,
            content_type="application/octet-stream",
        )
        print("parquet file profiled and saved")

        return True
    return False


def alter_table(filename, query, con):
    try:
        con.sql("drop table DATASET")
    except:
        print("table does not exist")

    con.sql(
        " create table DATASET as select * from read_parquet('s3://auctus-bucket/{}');".format(
            filename
        )
    )
    con.sql(query)
    con.sql(
        "COPY DATASET TO 's3://auctus-bucket/{}' (FORMAT 'parquet');".format(filename)
    )
    query_result = con.sql(
        " select * from read_parquet('s3://auctus-bucket/{}');".format(filename)
    )
    print(
        "COPY DATASET TO 's3://auctus-bucket/{}' (FORMAT 'parquet');".format(filename)
    )
    print("table successfuly altered")
    return query_result


def connect_duckdb():
    con = duckdb.connect("flask.db")
    con.sql("install 'httpfs';")
    con.sql("load 'httpfs';")
    con.sql("SET s3_url_style='path';")
    con.sql(" SET s3_endpoint='localhost:8050'")
    con.sql("SET s3_use_ssl=false;")
    con.sql("SET s3_access_key_id='devkey';")
    con.sql("SET s3_secret_access_key='devpassword';")
    return con


@app.route("/reset")
@cross_origin()
def reset():
    id = request.args.get("id")
    filename = id.replace(".", "_").replace("-", "_") + ".parquet"
    con = connect_duckdb()

    try:
        client.remove_object(bucket_name="auctus-bucket", object_name=filename)
    except:
        print("file not found")
    download_and_save(id)
    query_result = con.sql(
        " select * from read_parquet('s3://auctus-bucket/{}');".format(filename)
    )
    dtypes = query_result.dtypes
    query_result = query_result.fetchdf().to_json(orient="records", date_format="iso")
    con.close()
    return {
        "data": query_result,
        "error": False,
        "message": "success",
        "dtypes": dtypes,
    }


@app.route("/query")
@cross_origin()
def query():
    id = request.args.get("id")
    query = request.args.get("query")

    filename = id.replace(".", "_").replace("-", "_") + ".parquet"

    if not client.bucket_exists("auctus-bucket"):
        client.make_bucket("auctus-bucket")

    # bucket creation done
    try:
        response = client.get_object("auctus-bucket", filename)
        print("dtasaet found")
    except:
        if not download_and_save(id):
            return {
                "data": "[]",
                "error": True,
                "message": "Dataset is not downloadable",
                "dtypes": "[]",
            }

    con = connect_duckdb()
    query_result = []

    try:
        if not query:
            query_result = con.sql(
                " select * from read_parquet('s3://auctus-bucket/{}');".format(filename)
            )
        elif "alter table" in query.lower() or "update" in query.lower():
            query_result = alter_table(filename, query, con)

        else:
            query = query.replace(
                "DATASET", "read_parquet('s3://auctus-bucket/{}')".format(filename)
            )
            query_result = con.sql(query)

    except Exception as e:
        con.close()
        return {
            "data": "[]",
            "error": True,
            "message": f"Error in query: {e}",
            "dtypes": "[]",
        }

    dtypes = query_result.dtypes
    query_result = query_result.fetchdf().to_json(orient="records", date_format="iso")
    con.close()
    return {
        "data": query_result,
        "error": False,
        "message": "success",
        "dtypes": dtypes,
    }
    # return {"data":"[]","error":True,"message":"Error in query","dtypes":"[]"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
