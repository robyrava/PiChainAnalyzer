import config
from connectors.bitcoin_connector import BitcoinConnector
from connectors.neo4j_connector import Neo4jConnector
from etl.parser import process_block
from etl.clustering import *
from analysis.queries import get_fan_out_query, get_fan_in_query

def run_analysis(neo4j_conn):
    print("\n--- Avvio analisi dei pattern transazionali ---")

    # Analisi Fan-Out (Smurfing)
    print("\n[Ricerca] Pattern Fan-Out (potenziale Smurfing)...")
    fan_out_query = get_fan_out_query()
    # Ora 'records' è già una lista, non c'è più bisogno di 'list()'
    records = neo4j_conn.execute_query(fan_out_query, parameters={'max_inputs': 2, 'min_outputs': 10})

    if not records:
        print("Nessuna transazione con pattern Fan-Out trovata con i criteri attuali.")
    else:
        print("Trovate transazioni sospette:")
        for record in records:
            print(f"  - TXID: {record['txid']} (Inputs: {record['inputs']}, Outputs: {record['outputs']})")

    # Analisi Fan-In (Consolidamento)
    print("\n[Ricerca] Pattern Fan-In (potenziale Consolidamento)...")
    fan_in_query = get_fan_in_query()
    # Applichiamo la stessa semplificazione qui
    records = neo4j_conn.execute_query(fan_in_query, parameters={'min_inputs': 10, 'max_outputs': 2})

    if not records:
        print("Nessuna transazione con pattern Fan-In trovata con i criteri attuali.")
    else:
        print("Trovate transazioni sospette:")
        for record in records:
            print(f"  - TXID: {record['txid']} (Inputs: {record['inputs']}, Outputs: {record['outputs']})")

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
    run_analysis(neo4j_conn)

    # Chiudi connessioni
    neo4j_conn.close()
    print("\nProcesso ETL completato.")

if __name__ == "__main__":
    main()