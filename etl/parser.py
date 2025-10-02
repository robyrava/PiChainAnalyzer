from analysis.queries import (
    get_create_transaction_query,
    get_create_output_query,
    get_create_input_query,
    get_create_block_and_link_tx_query
)

def process_block(block_height,btc_connector, neo4j_connector):
    """
    Processa un singolo blocco, estraendo dati tramite il btc_connector
    e caricandoli tramite il neo4j_connector.
    """
    print(f"\n--- Inizio processamento del blocco {block_height} ---")
    try:
        block_data = btc_connector.get_block_by_height(block_height)
        transactions = block_data['tx']
        block_hash = block_data['hash']
        block_timestamp = block_data['time']

        total_transactions = len(transactions)
        print(f"Trovate {total_transactions} transazioni.")

        for i, tx in enumerate(transactions):
            print(f"  Processando transazione: {i + 1}/{total_transactions}  \r", end="", flush=True)

            tx_id = tx['txid']
            # 1. Usa la funzione per ottenere la query
            neo4j_connector.execute_query(
                get_create_transaction_query(),
                parameters={'tx_id': tx_id, 'h': block_height}
            )
            # 2. Crea il nodo Blocco e collega la transazione ad esso
            neo4j_connector.execute_query(
                get_create_block_and_link_tx_query(),
                parameters={
                    'tx_id': tx_id,
                    'block_height': block_height,
                    'block_hash': block_hash,
                    'timestamp': block_timestamp
                }
            )

            # 3. Processa output
            for vout in tx['vout']:
                if 'address' in vout['scriptPubKey']:
                    address = vout['scriptPubKey']['address']
                    value = vout['value']
                    # Usa la funzione per ottenere la query
                    neo4j_connector.execute_query(
                       get_create_output_query(),
                       parameters={'tx_id': tx_id, 'addr': address, 'val': float(value)}
                    )
            # 4. Processa input
            if not tx['vin'][0].get('coinbase'):
                for vin in tx['vin']:
                    source_tx = btc_connector.get_transaction(vin['txid'])
                    source_vout = source_tx['vout'][vin['vout']]
                    if 'address' in source_vout['scriptPubKey']:
                        address = source_vout['scriptPubKey']['address']
                        value = source_vout['value']
                        
                        # Calcolo dell'età
                        source_tx_timestamp = source_tx.get('time', block_timestamp)
                        age_in_seconds = block_timestamp - source_tx_timestamp
                        age_in_days = age_in_seconds / (60 * 60 * 24) # 86400 secondi in un giorno

                        neo4j_connector.execute_query(
                            get_create_input_query(),
                            parameters={'tx_id': tx_id, 'addr': address, 'val': float(value), 'age': age_in_days}
                        )
        print(f"--- Fine processamento del blocco {block_height} ---")
    except Exception as e:
        print(f"Errore durante il processamento del blocco {block_height}: {e}")