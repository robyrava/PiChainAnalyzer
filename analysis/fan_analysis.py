from .queries import get_fan_out_query, get_fan_in_query

def run(neo4j_conn):
    """
    Esegue l'analisi per identificare i pattern Fan-In e Fan-Out.
    """
    print("\n[Analisi] Ricerca Pattern Fan-Out (potenziale Smurfing)...")
    fan_out_query = get_fan_out_query()
    records = neo4j_conn.execute_query(fan_out_query, parameters={'max_inputs': 2, 'min_outputs': 10})

    if not records:
        print("  > Nessuna transazione con pattern Fan-Out trovata.")
    else:
        print("  > Trovate transazioni sospette:")
        for record in records:
            print(f"    - TXID: {record['txid']} (Inputs: {record['inputs']}, Outputs: {record['outputs']})")

    print("\n[Analisi] Ricerca Pattern Fan-In (potenziale Consolidamento)...")
    fan_in_query = get_fan_in_query()
    records = neo4j_conn.execute_query(fan_in_query, parameters={'min_inputs': 10, 'max_outputs': 2})

    if not records:
        print("  > Nessuna transazione con pattern Fan-In trovata.")
    else:
        print("  > Trovate transazioni sospette:")
        for record in records:
            print(f"    - TXID: {record['txid']} (Inputs: {record['inputs']}, Outputs: {record['outputs']})")