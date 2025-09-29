import os
from dotenv import load_dotenv

# Carica le variabili dal file .env
load_dotenv()

# Esponi le variabili di configurazione
RPC_USER = os.getenv("RPC_USER")
RPC_PASS = os.getenv("RPC_PASS")
RPC_HOST = os.getenv("RPC_HOST")
RPC_PORT = os.getenv("RPC_PORT")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASS = os.getenv("NEO4J_PASS")