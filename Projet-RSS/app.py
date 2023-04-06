from flask import Flask, render_template
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import tkinter as tk
import time

app = Flask(__name__, template_folder='templates')


# Configure Cassandra
contact_points = ['localhost']
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
session = cluster.connect('your_keyspace')

# Route to display Cassandra data in a table
@app.route('/')
def display_data():
    rows = session.execute("SELECT * FROM RSS")
    data = []
    for row in rows:
        data.append(row)
    return render_template('table.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)