import argparse
import sys
import config
from connectors.bitcoin_connector import BitcoinConnector
from connectors.neo4j_connector import Neo4jConnector
from etl.parser import process_block
from etl.clustering import *
from analysis import fan_analysis, peel_chain_analysis, dormant_funds_analysis, self_change_peel_analysis



def run_analysis(neo4j_conn, analysis_type='all', dormant_years=5):
    """
    Avvia l'esecuzione dei moduli di analisi in base al tipo scelto.
    """
    print(f"\n--- AVVIO FASE DI ANALISI (Tipo: {analysis_type.upper()}) ---")

    if analysis_type == 'fan' or analysis_type == 'all':
        fan_analysis.run(neo4j_conn)
    
    if analysis_type == 'peel-sc' or analysis_type == 'all':
        self_change_peel_analysis.run(neo4j_conn)
    
    if analysis_type == 'peel-heuristic' or analysis_type == 'all':
        peel_chain_analysis.run(neo4j_conn)
    
    if analysis_type == 'dormant' or analysis_type == 'all':
        dormant_funds_analysis.run(neo4j_conn, dormant_years)
    
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
    
    parser.add_argument(
        '--type',
        choices=['fan', 'peel-sc', 'peel-heuristic', 'dormant', 'all'],
        default='all',
        help="Specifica il tipo di analisi da eseguire (usato con --action analyze)."
    )

    parser.add_argument(
        '--years',
        type=int,
        default=5,
        help="Anni minimi di inattività per l'analisi dei fondi dormienti (default: 5)."
    )
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
        apply_common_input_ownership(neo4j_conn)
        run_analysis(neo4j_conn,args.type,args.years)

    # Chiudi connessioni
    neo4j_conn.close()
    print("\n--- Processo completato ---")

if __name__ == "__main__":
    main()
