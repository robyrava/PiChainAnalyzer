from .queries import get_self_change_peel_link_query, get_next_transaction_query

def run(neo4j_conn):
    """
    Esegue l'analisi "High-Confidence" per ricostruire le Peeling Chains
    basate sull'euristica del self-change address.
    """
    print("\n[Analisi High-Confidence] Ricostruzione Peeling Chains (Self-Change)...")
    
    # 1. Trova tutti gli anelli basati sul self-change
    self_change_query = get_self_change_peel_link_query()
    links_data = neo4j_conn.execute_query(self_change_query)
    peel_links_map = {record['txid']: record['change_address'] for record in links_data}
    
    if not peel_links_map:
        print("  > Nessun anello di Peeling Chain (Self-Change) trovato.")
        return

    print(f"  Trovati {len(peel_links_map)} possibili anelli (Self-Change). Inizio ricostruzione...")

    # 2. Logica di ricostruzione (identica a prima, ma su dati più affidabili)
    next_tx_query = get_next_transaction_query()
    found_chains = []
    processed_txids = set()

    for i, start_txid in enumerate(peel_links_map.keys()):
        print(f"  Analizzando punto di partenza: {i + 1}/{len(peel_links_map)}  \r", end="", flush=True)

        if start_txid in processed_txids:
            continue

        current_chain = [start_txid]
        current_txid = start_txid
        
        while True:
            if current_txid not in peel_links_map:
                break
            
            change_address = peel_links_map[current_txid]
            processed_txids.add(current_txid)
            
            next_tx_result = neo4j_conn.execute_query(next_tx_query, parameters={'address': change_address})
            if not next_tx_result:
                break
            
            next_txid = next_tx_result[0]['next_txid']

            if next_txid in peel_links_map and next_txid not in current_chain:
                current_chain.append(next_txid)
                current_txid = next_txid
            else:
                break
        
    min_chain_length = 2 # Per queste catene ad alta affidabilità, anche 2 anelli sono interessanti
    if len(current_chain) >= min_chain_length:
        found_chains.append(current_chain)

    print() 
    print("  Ricostruzione completata.")

    # 3. Stampa i risultati
    if not found_chains:
        print("  > Nessuna Peeling Chain completa (Self-Change) trovata.")
    else:
        print(f"  > Trovate {len(found_chains)} Peeling Chains complete (Self-Change):")
        found_chains.sort(key=len, reverse=True)
        
        for i, chain in enumerate(found_chains):
            print(f"\n    --- Catena #{i+1} (Lunghezza: {len(chain)}) ---")
            for j, txid in enumerate(chain):
                print(f"      {j+1}. {txid}")