from flask import (
    Flask,
    request,
    redirect,
    jsonify,
    render_template,
    send_from_directory,
)
from da import DeviantArt, populate

import os
from datetime import datetime
from sql import (
    top_by_activity,
    get_deviation_activity,
    get_publication_data,
    get_gallery_data,
    get_user_data,
)

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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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


@app.route("/update-table")
def update_table():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    limit = request.args.get("limit", 10)
    gallery = request.args.get("gallery")
    logger.info(f"Updating table for {start_date} to {end_date} with limit {limit}")

    if start_date:
        start_date = datetime.fromisoformat(start_date)

    if end_date:
        end_date = datetime.fromisoformat(end_date)

    table_data = top_by_activity(start_date, end_date, limit, gallery)

    return render_template("partials/table.html", table_data=table_data)


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
    return send_from_directory(
        os.path.join(os.getcwd(), "thumbs"), f"{deviation_id}.jpg"
    )


@app.route("/get-publication-data")
def get_by_publication_date():
    start_date = request.args.get("start_date")
    if start_date:
        start_date = datetime.fromisoformat(start_date)

    end_date = request.args.get("end_date")
    if end_date:
        end_date = datetime.fromisoformat(end_date)

    gallery = request.args.get("gallery")

    logger.info(f"Getting publication data for {start_date} to {end_date}")
    return jsonify(
        {"status": "success", "data": get_publication_data(start_date, end_date, gallery)}
    )


@app.route("/get-gallery-data")
def gallery_data():
    return jsonify({"status": "success", "data": get_gallery_data()})


@app.route("/get-users")
def get_users():

    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    limit = request.args.get("limit", 10)
    gallery = request.args.get("gallery")

    if start_date:
        start_date = datetime.fromisoformat(start_date)

    if end_date:
        end_date = datetime.fromisoformat(end_date)

    logger.info(f"Updating users for {start_date} to {end_date} with limit {limit} and gallery {gallery}")

    return jsonify(
        {"status": "success", "data": get_user_data(start_date, end_date, limit, gallery)}
    )


if __name__ == "__main__":
    # t = threading.Thread(target=populate_hourly)
    # t.daemon = True
    # t.start()

    # Enable debug logging for SQL queries
    logging.getLogger('sql').setLevel(logging.DEBUG)
    logging.getLogger('models').setLevel(logging.DEBUG)

    logger.info("Starting app")

    app.run(port=8080, debug=True)
