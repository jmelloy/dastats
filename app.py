from flask import Flask, request, redirect, jsonify, render_template
from da import DeviantArt
from models import Select, Deviation, DeviationActivity
import duckdb

import os
import json
from datetime import datetime

# Initialize Flask app
app = Flask(__name__)
da = DeviantArt()


@app.route("/")
def index():
    if not os.path.exists(".credentials.json"):
        return render_template("credentials.html")
    else:
        return redirect("/stats/")


# OAuth configuration
@app.route("/login", methods=["POST"])
def login():
    """Redirect to the OAuth provider's authorization URL."""

    client_id = request.form.get("client_id")
    client_secret = request.form.get("client_secret")

    if not client_id or not client_secret:
        return "Error: Missing client_id or client_secret."
    da.set_credentials(client_id, client_secret)

    return redirect(da.authorization_url())


@app.route("/callback")
def callback():
    """Handle the OAuth callback and exchange the code for a token."""
    code = request.args.get("code")
    if not code:
        return "Error: No code received."

    data = da.update_access_token(code)
    return jsonify(data)


conn = duckdb.connect("deviantart_data.db", read_only=True)


@app.route("/stats/")
def stats():
    return render_template("dashboard.html")


def query_database(start_time):
    # Connect to the database
    with open("sql/top10_activity.sql") as F:
        query = F.read()

    with duckdb.connect("deviantart_data.db", read_only=True) as conn:
        cursor = conn.cursor()

        query = query.format(start_date=start_time)

        cursor = conn.cursor()
        cursor.execute(query)
        columns = [col[0].lower() for col in cursor.description]

        # Convert the cursor to a list of dictionaries
        rs = []
        for row in cursor.fetchall():
            v = dict(zip(columns, row))

            v["total"] = sum([x["count"] for x in v["timestamps"]])

            rs.append(v)

        return rs


@app.route("/update-table", methods=["POST"])
def update_table():
    date_str = request.form.get("date")
    if date_str:
        date_object = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        table_data = query_database(date_object)
        print(table_data)
        return render_template("partials/table.html", table_data=table_data)
    return "Invalid date", 400


if __name__ == "__main__":
    app.run(port=8080, debug=True)
