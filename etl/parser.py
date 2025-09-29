from analysis.queries import (
    get_create_transaction_query,
    get_create_output_query,
    get_create_input_query
)

def process_block(block_height, btc_connector, neo4j_connector):
    """
    Processa un singolo blocco, estraendo dati tramite il btc_connector
    e caricandoli tramite il neo4j_connector.
    """
    print(f"\n--- Inizio processamento del blocco {block_height} ---")
    try:
        block_data = btc_connector.get_block_by_height(block_height)
        transactions = block_data['tx']
        print(f"Trovate {len(transactions)} transazioni.")

        for tx in transactions:
            tx_id = tx['txid']
            # 1. Usa la funzione per ottenere la query
            neo4j_connector.execute_query(
                get_create_transaction_query(),
                parameters={'tx_id': tx_id, 'h': block_height}
            )
            # 2. Processa output
            for vout in tx['vout']:
                if 'address' in vout['scriptPubKey']:
                    address = vout['scriptPubKey']['address']
                    value = vout['value']
                    # Usa la funzione per ottenere la query
                    neo4j_connector.execute_query(
                       get_create_output_query(),
                       parameters={'tx_id': tx_id, 'addr': address, 'val': float(value)}
                    )
            # 3. Processa input
            if not tx['vin'][0].get('coinbase'):
                for vin in tx['vin']:
                    source_tx = btc_connector.get_transaction(vin['txid'])
                    source_vout = source_tx['vout'][vin['vout']]
                    if 'address' in source_vout['scriptPubKey']:
                        address = source_vout['scriptPubKey']['address']
                        value = source_vout['value']
                        # Usa la funzione per ottenere la query
                        neo4j_connector.execute_query(
                            get_create_input_query(),
                            parameters={'tx_id': tx_id, 'addr': address, 'val': float(value)}
                        )
        print(f"--- Fine processamento del blocco {block_height} ---")
    except Exception as e:
        print(f"Errore durante il processamento del blocco {block_height}: {e}")