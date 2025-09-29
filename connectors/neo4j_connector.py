from neo4j import GraphDatabase

class Neo4jConnector:
    def __init__(self, uri, user, password):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            self._driver.verify_connectivity()
            print("Connesso a Neo4j.")
        except Exception as e:
            print(f"Errore di connessione a Neo4j: {e}")
            raise

    def close(self):
        if self._driver is not None:
            self._driver.close()
            print("Connessione a Neo4j chiusa.")

    def execute_query(self, query, parameters=None):
        """Esegue una query su Neo4j."""
        with self._driver.session() as session:
            return session.run(query, parameters)