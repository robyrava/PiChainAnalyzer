from analysis.queries import get_common_input_ownership_query

def apply_common_input_ownership(neo4j_connector):
    print("\n--- Applicazione dell'euristica 'Common-Input-Ownership' ---")

    try:
        # Usa la funzione per ottenere la query
        query = get_common_input_ownership_query()
        neo4j_connector.execute_query(query)
        print("Euristica applicata con successo. Create relazioni [:SAME_ENTITY].")
    except Exception as e:
        print(f"Errore durante l'applicazione dell'euristica: {e}")
