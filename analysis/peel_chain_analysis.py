from .queries import (
    get_peeling_chain_link_query, 
    get_next_transaction_query
)

def run(neo4j_conn):
    """
    Esegue l'analisi per ricostruire le Peeling Chains complete con una logica
    di collegamento più robusta.
    """
    print("\n[Analisi Avanzata] Ricostruzione Peeling Chains complete...")
    
    # 1. Trova tutti i possibili anelli e i loro indirizzi di resto in un colpo solo.
    # Modifichiamo la query get_peeling_chain_link_query per restituire anche il resto.
    # Per non modificare queries.py, la definiamo temporaneamente qui.
    peel_links_query_with_change = (
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)-[:RECEIVED]->() } = 2 AND COUNT { (t)<-[:SENT]-() } <= 2 "
        "WITH t, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t) | s] | total + s.value) AS total_input_value "
        "WHERE total_input_value >= 0.01 "
        "WITH t, total_input_value, [(t)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs "
        "WITH t, total_input_value, outputs[0] AS o1, outputs[1] AS o2 "
        "WITH t, total_input_value, o1, o2, (o1.val + o2.val) AS total_output_value "
        "WHERE total_output_value <= total_input_value "
        "AND ( (o1.val < total_input_value * 0.2 AND o2.val > total_input_value * 0.2) OR "
        "      (o2.val < total_input_value * 0.2 AND o1.val > total_input_value * 0.2) ) "
        "WITH t, CASE WHEN o1.val > o2.val THEN o1.addr.address ELSE o2.addr.address END AS change_address "
        "RETURN t.txid AS txid, change_address"
    )
    
    links_data = neo4j_conn.execute_query(peel_links_query_with_change)
    # Creiamo un dizionario per un accesso rapido: {txid: change_address}
    peel_links_map = {record['txid']: record['change_address'] for record in links_data}
    
    if not peel_links_map:
        print("  > Nessun anello di Peeling Chain trovato. Impossibile ricostruire catene.")
        return

    print(f"  Trovati {len(peel_links_map)} possibili anelli. Inizio ricostruzione catene...")

    # 2. Inizializza query di supporto
    next_tx_query = get_next_transaction_query()
    
    # 3. Logica di ricostruzione iterativa
    found_chains = []
    processed_txids = set()

    # Itera su tutti gli anelli trovati come possibili punti di partenza
    for i, start_txid in enumerate(peel_links_map.keys()):
        print(f"  Analizzando punto di partenza: {i + 1}/{len(peel_links_map)}  \r", end="", flush=True)

        if start_txid in processed_txids:
            continue

        current_chain = [start_txid]
        current_txid = start_txid
        
        while True:
            # L'anello corrente deve essere nella nostra mappa
            if current_txid not in peel_links_map:
                break
            
            change_address = peel_links_map[current_txid]
            processed_txids.add(current_txid)

            # Trova la transazione successiva che spende da quell'indirizzo
            next_tx_result = neo4j_conn.execute_query(next_tx_query, parameters={'address': change_address})
            
            if not next_tx_result:
                break # La catena finisce qui
            
            next_txid = next_tx_result[0]['next_txid']

            # CONTROLLO CHIAVE: la transazione successiva deve essere anch'essa un anello di peel valido
            if next_txid in peel_links_map and next_txid not in current_chain:
                current_chain.append(next_txid)
                current_txid = next_txid
            else:
                break # La catena si interrompe perché il prossimo anello non è valido o crea un ciclo
        
    min_chain_length = 3
    if len(current_chain) >= min_chain_length:
        found_chains.append(current_chain)

    print() 
    print("  Ricostruzione completata.")

    # 4. Stampa i risultati
    if not found_chains:
        print("  > Nessuna Peeling Chain completa (lunghezza >= 3) trovata.")
    else:
        print(f"  > Trovate {len(found_chains)} Peeling Chains complete:")
        found_chains.sort(key=len, reverse=True)
        
        for i, chain in enumerate(found_chains):
            print(f"\n    --- Catena #{i+1} (Lunghezza: {len(chain)}) ---")
            for j, txid in enumerate(chain):
                print(f"      {j+1}. {txid}")