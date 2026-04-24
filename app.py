from flask import Flask, request, jsonify
import requests
import os
import logging
from dotenv import load_dotenv

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("TOKEN")

def get_run_output(run_id):
    url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    
    response = requests.get(url, headers=headers, params={"run_id": run_id})
    return response.json()

def analyze_logs(logs):
    prompt = f"""
You are a Databricks troubleshooting assistant.

Analyze the following job failure logs:
{logs}

Provide:
1. Root cause
2. Category (data, infra, code, config)
3. Suggested fix
4. Confidence
"""

    response = requests.post(
        f"{DATABRICKS_HOST}/serving-endpoints/your-endpoint/invocations",
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        },
        json={"inputs": prompt}
    )

    return response.json()

@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.json

    run_id = payload.get("run", {}).get("run_id")
    logging.info(f"Received failure for run_id: {run_id}")

    if not run_id:
        return jsonify({"error": "run_id missing"}), 400

    # Fetch logs
    run_output = get_run_output(run_id)
    logs = run_output.get("error", "") or "No error logs found"

    # Analyze with LLM
    analysis = logs #analyze_logs(logs)

    return jsonify({
        "run_id": run_id,
        "analysis": analysis
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)