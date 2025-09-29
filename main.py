import config
from connectors.bitcoin_connector import BitcoinConnector
from connectors.neo4j_connector import Neo4jConnector
from etl.parser import process_block
from etl.clustering import *

def main():
    print("Avvio del processo ETL di Chainalysis.")

    # Inizializza i connettori
    try:
        btc_conn = BitcoinConnector(config.RPC_USER, config.RPC_PASS, config.RPC_HOST, config.RPC_PORT)
        neo4j_conn = Neo4jConnector(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASS)
    except Exception:
        print("Impossibile inizializzare i connettori. Uscita.")
        return

    # Logica principale: processa un range di blocchi
    start_block = 600000
    end_block = 600005 # Processiamo 5 blocchi per esempio

    """
    for height in range(start_block, end_block + 1):
        process_block(height, btc_conn, neo4j_conn)
    """
    
    h_block = 916863	
    process_block(h_block, btc_conn, neo4j_conn)
    apply_common_input_ownership(neo4j_conn)


    # Chiudi connessioni
    neo4j_conn.close()
    print("\nProcesso ETL completato.")

if __name__ == "__main__":
    main()