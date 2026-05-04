from flask import Flask, jsonify
from service import get_employees_for_each_job, list_ids_names_numbers

app = Flask(__name__)

@app.route("/hires-by-quarter")
def hires_by_quarter():
    return jsonify(get_employees_for_each_job())

@app.route("/depts-above-mean")
def depts_above_mean():
    return jsonify(list_ids_names_numbers())
    