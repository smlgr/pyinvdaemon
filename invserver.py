import psycopg2
from flask import Flask, request

from sql.preparing_cursor import PreparingCursor

app = Flask(__name__)

db_conn = psycopg2.connect(host="127.0.0.1", port=5432, user="smlgr", password="smlgr", dbname="smlgr")


@app.route(
    rule="/api/public/v1/data",
    methods=["POST"]
)
def data():
    request_body = request.json

    for item in ["ts", "dc_voltage", "dc_current", "ac_voltage", "ac_current", "power", "frequency"]:
        if item not in request_body:
            raise ValueError("Invalid data. %s not found" % item)

    db_cursor = db_conn.cursor(cursor_factory=PreparingCursor)

    db_cursor.prepare("""INSERT INTO data 
        (dc_voltage, dc_current, ac_voltage, ac_current, power, frequency)
        VALUES (
            %(dc_voltage)s::integer,
            %(dc_current)s::integer,
            %(ac_voltage)s::integer,
            %(ac_current)s::integer,
            %(power)s::integer,
            %(frequency)s::integer
        );""")

    db_cursor.execute({
        "dc_voltage": float(data["UDC"]) / 10,
        "dc_current": float(data["IDC"]) / 100,
        "ac_voltage": float(data["UL1"]) / 10,
        "ac_current": float(data["IL1"]) / 100,
        "power": float(data["PAC"]) / 10,
        "frequency": float(data["TNF"]) / 100
    })

    db_conn.commit()

    return {}


if __name__ == "__main__":
    app.run()
