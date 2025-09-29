from bitcoinrpc.authproxy import AuthServiceProxy

class BitcoinConnector:
    def __init__(self, user, password, host, port):
        self.rpc_url = f"http://{user}:{password}@{host}:{port}"
        try:
            self.rpc = AuthServiceProxy(self.rpc_url, timeout=120)
            self.rpc.getblockcount() # Test della connessione
            print("Connesso al nodo Bitcoin.")
        except Exception as e:
            print(f"Errore di connessione al nodo Bitcoin: {e}")
            raise

    def get_block_by_height(self, height):
        """Recupera un intero blocco data l'altezza."""
        block_hash = self.rpc.getblockhash(height)
        return self.rpc.getblock(block_hash, 2) # Verbosity 2

    def get_transaction(self, txid):
        """Recupera una singola transazione dato il suo txid."""
        return self.rpc.getrawtransaction(txid, True) # Verbose
