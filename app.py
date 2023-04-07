from flask import Flask, render_template
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import tkinter as tk
import pandas as pd
from RSS_data import get_source, get_feed, ouvrir_fenetre, get_dataframes, data_cleaning, filter_sort_data, kafka_canssandra, close_connection

from bs4 import XMLParsedAsHTMLWarning
import warnings
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
warnings.filterwarnings("ignore", message="La méthode frame.append est obsolète et sera supprimée des pandas dans le futur version. Utilisez pandas.concat à la place.")
warnings.filterwarnings("ignore", message="The frame.append method is deprecated and will be removed from pandas in a future version. Use pandas.concat instead.")



app = Flask(__name__, template_folder='templates')

# Configure Cassandra
contact_points = ['localhost']
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
session = cluster.connect('your_keyspace')


# Route to display Cassandra data in a table
@app.route('/')
def display_data():
    
    # Appel de la fonction
    urls = ouvrir_fenetre()
    print(urls)

    # Récupération des données
    data = get_dataframes(urls)

    # Nettoyage des données
    data = data_cleaning(data)

    # Tri des données
    data = filter_sort_data(data)

    # Configuration de Kafka et Cassandra
    session, producer, cluster = kafka_canssandra(data)

    rows = session.execute("SELECT * FROM RSS")
    data = []
    for row in rows:
        data.append(row)

    # Fermeture de la connexion
    close_connection(producer, session, cluster)

    return render_template('table.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)