"""
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
        "MERGE (a:Address {address: $addr}) "
        "MERGE (t)-[:RECEIVED {value: $val}]->(a)"
    )

def get_create_input_query():
    """Query per creare un nodo Indirizzo per un input e la relazione SENT."""
    return (
        "MATCH (t:Transaction {txid: $tx_id}) "
        "MERGE (a:Address {address: $addr}) "
        "MERGE (a)-[s:SENT {value: $val}]->(t) "
        "SET s.age_days = $age"
    )

def get_create_block_and_link_tx_query():
    """
    Crea un nodo Blocco (se non esiste) e collega la transazione ad esso.
    Versione corretta con SET per garantire l'idempotenza.
    """
    return (
        "MERGE (b:Block {height: $block_height}) "
        "SET b.hash = $block_hash, b.timestamp = datetime({epochSeconds: $timestamp}) "
        "WITH b "
        "MATCH (t:Transaction {txid: $tx_id}) "
        "MERGE (t)-[:INCLUDED_IN]->(b)"
    )

def get_dormant_funds_query(min_age_days=365):
    """
    Trova transazioni che spendono fondi rimasti inattivi per un
    determinato numero di giorni.
    """
    return (
        "MATCH (a:Address)-[s:SENT]->(t:Transaction) "
        "WHERE s.age_days >= $min_age_days "
        "RETURN t.txid AS txid, a.address AS from_address, s.value AS value, s.age_days AS days_dormant "
        "ORDER BY days_dormant DESC"
    )

# --- Query di Clustering ---
def get_common_input_ownership_query():
    """
    Query ottimizzata per applicare l'euristica common-input-ownership.
    """
    return (
        "MATCH (t:Transaction) "
        "WHERE COUNT { (:Address)-[:SENT]->(t) } > 1 "
        "WITH t "
        "MATCH (a1:Address)-[:SENT]->(t), (a2:Address)-[:SENT]->(t) "
        "WHERE elementId(a1) < elementId(a2) "
        "MERGE (a1)-[:SAME_ENTITY]-(a2)"
    )


# --- Query di Analisi Pattern ---
def get_fan_out_query(min_outputs=10, max_inputs=2):
    """
    Trova transazioni di "distribuzione" (fan-out), potenziale smurfing.
    """
    return (
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)<-[:SENT]-() } <= $max_inputs "
        "AND COUNT { (t)-[:RECEIVED]->() } >= $min_outputs "
        "RETURN t.txid AS txid, COUNT { (t)<-[:SENT]-() } AS inputs, COUNT { (t)-[:RECEIVED]->() } AS outputs"
    )

def get_fan_in_query(min_inputs=10, max_outputs=2):
    """
    Trova transazioni di "consolidamento" (fan-in), potenziale sweep.
    """
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
        "MATCH (t:Transaction) "
        "WHERE COUNT { (t)-[:RECEIVED]->() } = 2 "
        "AND COUNT { (t)<-[:SENT]-() } <= 2 "
        "WITH t, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t) | s] | total + s.value) AS total_input_value "
        "WHERE total_input_value >= $min_input_value "
        "WITH t, total_input_value, [(t)-[r:RECEIVED]->(a:Address) | r.value] AS outputs "
        "WITH t, total_input_value, outputs[0] AS v1, outputs[1] AS v2 "
        "WITH t, total_input_value, v1, v2, (v1 + v2) AS total_output_value "
        "WHERE total_output_value <= total_input_value "
        "AND ( "
        "  (v1 < total_input_value * $peel_ratio AND v2 > total_input_value * $peel_ratio) OR "
        "  (v2 < total_input_value * $peel_ratio AND v1 > total_input_value * $peel_ratio) "
        ") "
        "RETURN t.txid AS txid, v1, v2, total_input_value"
    )

def get_full_peeling_chain_query(min_chain_length=3, peel_ratio=0.2, min_input_value=0.01):
    """
    Versione OTTIMIZZATA e CORRETTA che ricostruisce le peeling chains.
    """
    return (
        "MATCH (t1:Transaction) "
        "WHERE COUNT { (t1)-[:RECEIVED]->() } = 2 AND COUNT { (t1)<-[:SENT]-() } <= 2 "
        "WITH t1, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t1) | s] | total + s.value) AS total_input1 "
        "WHERE total_input1 >= $min_input_value "
        "WITH t1, total_input1, [(t1)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs1 "
        "WITH t1, total_input1, outputs1[0] AS o1, outputs1[1] AS o2 "
        "WHERE (o1.val < total_input1 * $peel_ratio AND o2.val > total_input1 * $peel_ratio) OR (o2.val < total_input1 * $peel_ratio AND o1.val > total_input1 * $peel_ratio) "
        "WITH t1, CASE WHEN o1.val > o2.val THEN o1.addr ELSE o2.addr END AS change_addr1 "
        "MATCH (change_addr1)-[:SENT]->(t2:Transaction) "
        # Aggiunto controllo per evitare catene A -> A
        "WHERE t1 <> t2 "
        "AND COUNT { (t2)-[:RECEIVED]->() } = 2 AND COUNT { (t2)<-[:SENT]-() } <= 2 "
        "WITH t1, t2, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t2) | s] | total + s.value) AS total_input2 "
        "WHERE total_input2 >= $min_input_value "
        "WITH t1, t2, total_input2, [(t2)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs2 "
        "WITH t1, t2, total_input2, outputs2[0] AS o3, outputs2[1] AS o4 "
        "WHERE (o3.val < total_input2 * $peel_ratio AND o4.val > total_input2 * $peel_ratio) OR (o4.val < total_input2 * $peel_ratio AND o3.val > total_input2 * $peel_ratio) "
        "WITH [t1.txid, t2.txid] AS chain_txids "
        "RETURN DISTINCT chain_txids"
    )

#----------- Peeling chain 

def get_peel_link_details_query(peel_ratio=0.2, min_input_value=0.01):
    """
    Dato un TXID, verifica se è un anello di peel e restituisce il resto.
    Versione corretta.
    """
    # --- CORREZIONE: Usa o1.val e o2.val invece di v1 e v2 nella clausola WHERE ---
    return (
        "MATCH (t:Transaction {txid: $txid}) "
        "WHERE COUNT { (t)-[:RECEIVED]->() } = 2 AND COUNT { (t)<-[:SENT]-() } <= 2 "
        "WITH t, REDUCE(total = 0.0, s IN [(a:Address)-[s:SENT]->(t) | s] | total + s.value) AS total_input_value "
        "WHERE total_input_value >= $min_input_value "
        "WITH t, total_input_value, [(t)-[r:RECEIVED]->(a:Address) | {addr: a, val: r.value}] AS outputs "
        "WITH t, total_input_value, outputs[0] AS o1, outputs[1] AS o2 "
        "WITH t, total_input_value, o1, o2, (o1.val + o2.val) AS total_output_value "
        "WHERE total_output_value <= total_input_value "
        "AND ( (o1.val < total_input_value * $peel_ratio AND o2.val > total_input_value * $peel_ratio) OR (o2.val < total_input_value * $peel_ratio AND o1.val > total_input_value * $peel_ratio) ) "
        "RETURN CASE WHEN o1.val > o2.val THEN o1.addr.address ELSE o2.addr.address END AS change_address"
    )

def get_next_transaction_query():
    """
    Dato un indirizzo, trova il TXID della transazione che lo usa come input.
    """
    return (
        "MATCH (a:Address {address: $address})-[:SENT]->(t:Transaction) "
        "RETURN t.txid AS next_txid LIMIT 1"
    )

def get_self_change_peel_link_query():
    """
    Trova anelli di peeling chain basati sull'euristica "self-change".
    Cerca transazioni 1-input/2-output dove un output torna all'indirizzo di input.
    """
    return (
        # Trova transazioni con 1 input e 2 output
        "MATCH (in_addr:Address)-[:SENT]->(t:Transaction) "
        "WHERE COUNT { (:Address)-[:SENT]->(t) } = 1 AND COUNT { (t)-[:RECEIVED]->() } = 2 "
        
        # Verifica che uno degli output sia un self-change
        "AND EXISTS ((t)-[:RECEIVED]->(in_addr)) "
        
        # Identifica l'indirizzo "peeled" (quello che non è il self-change)
        "WITH t, in_addr "
        "MATCH (t)-[:RECEIVED]->(peeled_addr:Address) "
        "WHERE peeled_addr <> in_addr "
        
        "RETURN t.txid AS txid, in_addr.address AS change_address"
    )

