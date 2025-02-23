from flask import Flask, request, redirect, jsonify, render_template, send_from_directory
from da import DeviantArt, populate

import os
from datetime import datetime
from sql import top_by_activity, get_deviation_activity

import multiprocessing
import threading
import time
import json

# Initialize Flask app
app = Flask(__name__)
da = DeviantArt()

import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

p = None


def populate_da():
    global p
    try:
        da.check_token()
        p = multiprocessing.Process(target=populate, args=(da,))
        p.daemon = True
        p.start()
    except Exception as e:
        print(e)


def populate_hourly():
    while True:
        if p:
            p.join()
        else:
            try:
                populate_da()
            except Exception as e:
                print(e)
        time.sleep(3600)


@app.route("/")
def index():
    global da

    if not os.path.exists(".credentials.json"):
        return render_template("credentials.html")
    
    da = DeviantArt()

    logger.info("Credentials set")

    if not os.path.exists(".token.json"):
        logger.info("No token found, redirecting to authorization")
        return redirect(da.authorization_url())

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

    logger.info(f"Code received: {code}")

    data = da.update_access_token(code)
    logger.info(f"Token updated: {data}")
    return redirect("/stats/")


@app.route("/stats/")
def stats():
    return render_template(
        "dashboard.html",
    )


@app.route("/update-table", methods=["POST"])
def update_table():
    date_str = request.form.get("date")
    limit = request.form.get("limit", 10)

    if date_str:
        date_object = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        table_data = top_by_activity(date_object, limit)

        return render_template("partials/table.html", table_data=table_data)
    return "Invalid date", 400


@app.route("/get-sparkline-data", methods=["POST"])
def get_sparkline_data():
    event_id = request.json.get("id")
    date_str = request.json.get("date")
    if event_id:
        # Simulate event-specific data points
        sparkline_data = get_deviation_activity(event_id, date_str)
        return jsonify({"status": "success", "data": sparkline_data})
    return jsonify({"status": "error", "message": "Invalid event ID"}), 400


@app.route("/thumbs/<deviation_id>")
def thumbs(deviation_id):
    return send_from_directory(os.path.join(os.getcwd(), "thumbs"), f"{deviation_id}.jpg")

if __name__ == "__main__":
    t = threading.Thread(target=populate_hourly)
    t.daemon = True
    t.start()

    logger.info("Starting app")

    app.run(port=8080, debug=True)
