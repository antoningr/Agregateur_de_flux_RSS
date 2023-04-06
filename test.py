from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Configure Cassandra
contact_points = ['localhost']
auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
session = cluster.connect('your_keyspace')

# Function to test Cassandra connection
def test_cassandra_connection():
    try:
        # Execute a test query
        session.execute("SELECT * FROM RSS LIMIT 1")
        print("Cassandra connection successful")
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")

test_cassandra_connection()