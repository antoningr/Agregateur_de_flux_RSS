from kafka import KafkaProducer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import tkinter as tk

import requests
import json
import pandas as pd
from datetime import datetime
from requests_html import HTML
from requests_html import HTMLSession

from sites import sites


# Retourne le code source de l'URL qui est passé en paramètre
def get_source(url):
    try:
        session = HTMLSession()
        response = session.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)


# Retourne un dataFrame qui contient le contenu du flux RSS passé en paramètre
def get_feed(url):   
    response = get_source(url)
    
    df = pd.DataFrame(columns = ['title', 'pubDate', 'link', 'description'])

    with response as r:
        items = r.html.find("item", first=False)

        for item in items:        

            title = item.find('title', first=True).text
            pubDate = item.find('pubDate', first=True).text
            link = item.find('guid', first=True).text
            description = item.find('description', first=True).text


            row = {'title': title, 'pubDate': pubDate, 'link': link, 'description': description}
            df = df.append(row, ignore_index=True)

    return df


# Permet à l'utilisateur de sélectionner les sites internet qu'il veut consulter parmi une liste prédéfinie
# Retourne une liste qui contient ces sites 
def ouvrir_fenetre():
    def ajouter_site():
        selection.clear()
        for site in listebox.curselection():
            nom_site = listebox.get(site)
            selection.append(sites[nom_site])
        fenetre.quit()

    fenetre = tk.Tk()

    listebox = tk.Listbox(fenetre, selectmode=tk.MULTIPLE)
    for site in sites:
        listebox.insert(tk.END, site)

    bouton_ajouter = tk.Button(fenetre, text="Ajouter", command=ajouter_site)
    selection = []
    listebox.grid(row=0, column=0)
    bouton_ajouter.grid(row=1, column=0)
    fenetre.mainloop()

    return selection


# Création du dataFrame en fonction des sites sélectionnés
def get_dataframes(urls):

    # Création d'une liste qui contiendra les dataFrame
    dfs = []

    # Récupération des dataFrame
    for url in urls:
        df = get_feed(url)
        dfs.append(pd.DataFrame(df))

    # Fusion des dataFrame en un seul
    data = pd.concat(dfs, ignore_index=True)

    return data


# Nettoyage des données du dataFrame
def data_cleaning(data) :

    # Suppression des caractères spéciaux
    data['title'] = data['title'].apply(lambda x: x.replace('ê', 'e'))
    data['title'] = data['title'].apply(lambda x: x.replace("'", ""))
    data['description'] = data['description'].apply(lambda x: x.replace("'", ""))
    
    # Suppression des balises HTML
    for i in range(len(data)):
        data['description'] = data['description'].apply(lambda x: x.replace('ê', 'e'))
        if '[' in data.loc[i,'title'] and ']' in data.loc[i,'title']:
            data.loc[i,'title'] = data.loc[i,'title'].split('<![CDATA[')[1].split(']')[0] 
        if '[' in data.loc[i,'description'] and ']' in data.loc[i,'description']:
            data.loc[i,'description'] = data.loc[i,'description'].split('<![CDATA[')[1].split(']')[0] 
        else:
            data.loc[i,'title'] = data.loc[i,'title']
            data.loc[i,'description'] = data.loc[i,'description']
    return data


# Classement des articles par ordre décroissant (plus récents en premier)
def filter_sort_data(data):
    # Convertir la colonne pubDate en datetime
    data['pubDate'] = pd.to_datetime(data['pubDate'], errors='coerce', format='%a, %d %b %Y %H:%M:%S %z')
    
    # Tri des articles par ordre décroissant
    data = data.sort_values(by='pubDate', ascending=False)
    
    return data


def kafka_canssandra(data):
    # Configure Kafka
    bootstrap_servers = ['localhost:9092']
    topic = 'your_kafka_topic'
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Configure Cassandra
    contact_points = ['localhost']
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
    session = cluster.connect()

    # Envoie du dataFrame vers Kafka
    serialized_df = data.to_json().encode('utf-8')  # encode the JSON string to bytes
    producer.send(topic, value=serialized_df)

    # Le dataFrame est ensuite stocké dans la base de données Cassandra
    session.execute("CREATE KEYSPACE IF NOT EXISTS your_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")
    session.execute("USE your_keyspace")
    session.execute("CREATE TABLE IF NOT EXISTS RSS (title text, pubdate text, link text, description text, PRIMARY KEY(title, pubdate)) WITH CLUSTERING ORDER BY (pubdate DESC)")

    # Insertion des données dans la base de données
    for _, row in data.iterrows():
        session.execute(f"INSERT INTO RSS (title, pubdate, link, description) VALUES ('{row['title']}', '{row['pubDate']}', '{row['link']}', '{row['description']}')")
    rows = session.execute("SELECT * FROM RSS")

    return session, producer, cluster


# Ferme la connection
def close_connection(producer, session, cluster):
    producer.flush()
    producer.close()
    session.shutdown()
    cluster.shutdown()
    producer.flush()
    producer.close()
    session.shutdown()
    cluster.shutdown()

# Suppression des données dans la basse de données
def delete_data(session):
    session.execute("DESCRIBE KEYSPACES;")
    session.execute("USE your_keyspace;")
    session.execute("DESCRIBE TABLES;")
    session.execute("TRUNCATE RSS")