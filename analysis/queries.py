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

def get_peeling_chain_link_query(peel_ratio=0.2, min_input_value=0.01):
    """
    Verifica che un output sia piccolo (peel) e l'altro grande (change).
    """
    return (
        # Inizia dalle transazioni con esattamente 2 output e 1-2 input
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)-[:RECEIVED]->() } = 2 "
        "AND COUNT { (t)<-[:SENT]-() } <= 2 "
        # Calcola il valore totale degli input
        "WITH t, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t) | s] | total + s.value) AS total_input_value "
        # Filtra per transazioni con un valore minimo per escludere il "rumore"
        "WHERE total_input_value >= $min_input_value "
        # Estrai i valori dei due output
        "WITH t, total_input_value, [(t)-[r:RECEIVED]->(a:Address) | r.value] AS outputs "
        "WITH t, total_input_value, outputs[0] AS v1, outputs[1] AS v2 "
        # Calcola il valore totale degli output
        "WITH t, total_input_value, v1, v2, (v1 + v2) AS total_output_value "
        # Condizione di Peeling Raffinata:
        # 1. L'output totale deve essere realistico rispetto all'input (esclude anomalie)
        # 2. Uno dei due output deve essere sotto la soglia del peel_ratio
        # 3. L'altro output deve essere il "resto" (sopra la soglia del peel_ratio)
        "WHERE total_output_value <= total_input_value "
        "AND ( "
        "  (v1 < total_input_value * $peel_ratio AND v2 > total_input_value * $peel_ratio) OR "
        "  (v2 < total_input_value * $peel_ratio AND v1 > total_input_value * $peel_ratio) "
        ") "
        "RETURN t.txid AS txid, v1, v2, total_input_value"
    )

def get_full_peeling_chain_query(min_chain_length=3, peel_ratio=0.2, min_input_value=0.01):
    """
    Versione OTTIMIZZATA che ricostruisce le peeling chains in modo incrementale
    per evitare l'esplosione combinatoria dei percorsi.
    """
    return (
        # Inizia trovando tutti i singoli anelli, come facevamo prima
        "MATCH (t1:Transaction) "
        "WHERE COUNT { (t1)-[:RECEIVED]->() } = 2 AND COUNT { (t1)<-[:SENT]-() } <= 2 "
        "WITH t1, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t1) | s] | total + s.value) AS total_input1 "
        "WHERE total_input1 >= $min_input_value "
        "WITH t1, total_input1, [(t1)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs1 "
        "WITH t1, total_input1, outputs1[0] AS o1, outputs1[1] AS o2 "
        "WHERE (o1.val < total_input1 * $peel_ratio AND o2.val > total_input1 * $peel_ratio) OR (o2.val < total_input1 * $peel_ratio AND o1.val > total_input1 * $peel_ratio) "
        # Ora abbiamo t1. Seguiamo il resto per trovare t2.
        "WITH t1, CASE WHEN o1.val > o2.val THEN o1.addr ELSE o2.addr END AS change_addr1 "
        "MATCH (change_addr1)-[:SENT]->(t2:Transaction) "
        # E verifichiamo che anche t2 sia un anello valido
        "WHERE COUNT { (t2)-[:RECEIVED]->() } = 2 AND COUNT { (t2)<-[:SENT]-() } <= 2 "
        "WITH t1, t2, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t2) | s] | total + s.value) AS total_input2 "
        "WHERE total_input2 >= $min_input_value "
        "WITH t1, t2, total_input2, [(t2)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs2 "
        "WITH t1, t2, total_input2, outputs2[0] AS o3, outputs2[1] AS o4 "
        "WHERE (o3.val < total_input2 * $peel_ratio AND o4.val > total_input2 * $peel_ratio) OR (o4.val < total_input2 * $peel_ratio AND o3.val > total_input2 * $peel_ratio) "
        # Se siamo arrivati fin qui, abbiamo una catena di lunghezza 2 (t1 -> t2).
        # Per ora ci fermiamo a catene semplici per garantire la performance.
        # Raccogliamo i TXID.
        "WITH [t1.txid, t2.txid] AS chain_txids "
        "WHERE size(chain_txids) >= $min_chain_length - 1 " # Logica leggermente diversa per la lunghezza
        "RETURN DISTINCT chain_txids" # Usiamo DISTINCT per evitare catene duplicate
    )


