import argparse
import sys
import config
from connectors.bitcoin_connector import BitcoinConnector
from connectors.neo4j_connector import Neo4jConnector
from etl.parser import process_block
from etl.clustering import *
from analysis.queries import get_fan_out_query, get_fan_in_query, get_peeling_chain_link_query, get_full_peeling_chain_query



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
    
    # Analisi Peeling Chain
    print("\n[Ricerca] Pattern Peeling Chain (potenziale Offuscamento)...")
    peeling_query = get_peeling_chain_link_query()
    # Usiamo un peel_ratio del 20% per essere più inclusivi
    records = neo4j_conn.execute_query(peeling_query, parameters={'peel_ratio': 0.2, 'min_input_value': 0.01})

    if not records:
        print("Nessun anello di Peeling Chain trovato con i criteri attuali.")
    else:
        print("Trovate transazioni sospette (anelli di Peeling Chain):")
        for record in records:
            print(f"  - TXID: {record['txid']} (Input: {record['total_input_value']:.4f} BTC, Outputs: {record['v1']:.4f} / {record['v2']:.4f} BTC)")
    
    # --- ANALISI PEELING CHAIN AVANZATA ---
    print("\n[Ricerca Avanzata] Ricostruzione Peeling Chains complete...")
    full_chain_query = get_full_peeling_chain_query()
    # Cerchiamo catene di almeno 3 transazioni
    records = neo4j_conn.execute_query(
        full_chain_query, 
        parameters={'min_chain_length': 3, 'peel_ratio': 0.2, 'min_input_value': 0.01}
    )

    if not records:
        print("Nessuna Peeling Chain completa trovata con i criteri attuali.")
    else:
        print("Trovate Peeling Chains complete:")
        chain_count = 1
        for record in records:
            print(f"\n  --- Catena #{chain_count} ---")
            tx_index = 1
            for txid in record['chain_txids']:
                print(f"    {tx_index}. {txid}")
                tx_index += 1
            chain_count += 1


def main():
    parser = argparse.ArgumentParser(
        description="PiChainAnalyzer - Strumento di Analisi Blockchain",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        '--action', 
        required=True, 
        choices=['etl', 'analyze'], 
        help=(
            "'etl': Esegue l'estrazione dei blocchi, il clustering e l'analisi.\n"
            "'analyze': Esegue solo clustering e analisi sui dati esistenti."
        )
    )
    parser.add_argument('--start-block', type=int, help="Il blocco di partenza per l'ETL.")
    parser.add_argument('--end-block', type=int, help="Il blocco di fine per l'ETL.")
    
    args = parser.parse_args()

    # Controllo logico: se l'azione è 'etl', start e end block sono obbligatori
    if args.action == 'etl' and (args.start_block is None or args.end_block is None):
        print("Errore: Per l'azione 'etl' è necessario specificare --start-block e --end-block.")
        sys.exit(1)
    if args.action == 'etl' and args.start_block > args.end_block:
        print("Errore: --start-block non può essere maggiore di --end-block.")
        sys.exit(1)

    print(f"Avvio del processo in modalità: {args.action.upper()}")
    
    try:
        neo4j_conn = Neo4jConnector(config.NEO4J_URI, config.NEO4J_USER, config.NEO4J_PASS)
    except Exception:
        print("Impossibile inizializzare il connettore Neo4j. Uscita.")
        return

    if args.action == 'etl':
        print(f"\n--- FASE ETL: Dal blocco {args.start_block} al {args.end_block} ---")
        try:
            btc_conn = BitcoinConnector(config.RPC_USER, config.RPC_PASS, config.RPC_HOST, config.RPC_PORT)
        except Exception:
            print("Impossibile inizializzare il connettore Bitcoin. Uscita.")
            neo4j_conn.close()
            return
            
        for height in range(args.start_block, args.end_block + 1):
            process_block(height, btc_conn, neo4j_conn)
        
        print("\n--- FASE DI CLUSTERING ---")
        apply_common_input_ownership(neo4j_conn)
        
        run_analysis(neo4j_conn)

    elif args.action == 'analyze':
        print("\n--- FASE DI CLUSTERING E ANALISI ---")
        run_analysis(neo4j_conn)

    # Chiudi connessioni
    neo4j_conn.close()
    print("\n--- Processo completato ---")

if __name__ == "__main__":
    main()
