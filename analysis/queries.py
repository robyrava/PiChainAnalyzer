""""
Questo modulo funge da libreria centralizzata per tutte le query Cypher
utilizzate nel progetto.
"""

# --- Query di Manutenzione ---
def get_clear_database_query():
    """Restituisce la query per cancellare tutti i nodi e le relazioni."""
    return "MATCH (n) DETACH DELETE n"

# --- Query ETL (Parser) ---
def get_create_transaction_query():
    """Query per creare un nodo Transazione."""
    return "MERGE (t:Transaction {txid: $tx_id}) SET t.block_height = $h"

def get_create_output_query():
    """Query per creare un nodo Indirizzo per un output e la relazione RECEIVED."""
    return (
        "MATCH (t:Transaction {txid: $tx_id}) "
        "MERge (a:Address {address: $addr}) "
        "MERGE (t)-[:RECEIVED {value: $val}]->(a)"
    )

def get_create_input_query():
    """Query per creare un nodo Indirizzo per un input e la relazione SENT."""
    return (
        "MATCH (t:Transaction {txid: $tx_id}) "
        "MERGE (a:Address {address: $addr}) "
        "MERGE (a)-[:SENT {value: $val}]->(t)"
    )

# --- Query di Clustering ---
def get_common_input_ownership_query():
    """Query per applicare l'euristica common-input-ownership."""
    return (
        "MATCH (t:Transaction) "
        "WITH t, [(a)-[:SENT]->(t) | a] AS input_addresses "
        "WHERE size(input_addresses) > 1 "
        "UNWIND input_addresses AS addr1 "
        "UNWIND input_addresses AS addr2 "
        "WITH addr1, addr2 WHERE elementId(addr1) < elementId(addr2) "
        "MERGE (addr1)-[:SAME_ENTITY]-(addr2)"
    )

def get_fan_out_query(min_outputs=10, max_inputs=2):
    """
    Trova transazioni di "distribuzione" (fan-out), potenziale smurfing.
    Cerca transazioni con pochi input e molti output.
    """
    # AGGIORNATO: Sostituito size() con COUNT {}
    return (
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)<-[:SENT]-() } <= $max_inputs "
        "AND COUNT { (t)-[:RECEIVED]->() } >= $min_outputs "
        "RETURN t.txid AS txid, COUNT { (t)<-[:SENT]-() } AS inputs, COUNT { (t)-[:RECEIVED]->() } AS outputs"
    )

def get_fan_in_query(min_inputs=10, max_outputs=2):
    """
    Trova transazioni di "consolidamento" (fan-in), potenziale sweep.
    Cerca transazioni con molti input e pochi output.
    """
    # AGGIORNATO: Sostituito size() con COUNT {}
    return (
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)<-[:SENT]-() } >= $min_inputs "
        "AND COUNT { (t)-[:RECEIVED]->() } <= $max_outputs "
        "RETURN t.txid AS txid, COUNT { (t)<-[:SENT]-() } AS inputs, COUNT { (t)-[:RECEIVED]->() } AS outputs"
    )

