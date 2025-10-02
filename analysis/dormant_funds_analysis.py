from .queries import get_dormant_funds_query

def run(neo4j_conn,min_age_years=5):
    """
    Esegue l'analisi per identificare il movimento di fondi dormienti.
    """
    print("\n[Analisi] Ricerca Movimento di Fondi Dormienti...")

    min_age_days = min_age_years * 365
    
    dormant_query = get_dormant_funds_query()
    records = neo4j_conn.execute_query(
        dormant_query,
        parameters={'min_age_days': min_age_days}
    )

    if not records:
        print(f"  > Nessun movimento di fondi dormienti (oltre {min_age_years} anni) trovato.")
    else:
        print(f"  > Trovati movimenti sospetti di fondi dormienti (oltre {min_age_years} anni):")
        for record in records:
            print(f"    - TXID: {record['txid']} ha speso {record['value']:.4f} BTC "
                  f"dall'indirizzo {record['from_address']} dopo {record['days_dormant']:.0f} giorni.")