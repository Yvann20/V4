import os
import sys
import signal
import logging
from rq import Worker, Queue
from redis import Redis, ConnectionPool
from dotenv import load_dotenv

# Configurar logging básico
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [PID:%(process)d] - %(message)s',
    handlers=[
        logging.FileHandler('logs/worker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Criar diretório de logs se não existir
os.makedirs('logs', exist_ok=True)

# Carregar variáveis de ambiente
load_dotenv()

# Configurações Redis
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Configurar pool de conexões Redis
redis_pool = ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    socket_timeout=10,
    socket_connect_timeout=5,
    retry_on_timeout=True
)

# Conexão Redis
redis_conn = Redis(connection_pool=redis_pool)

# Verificar conexão com Redis
try:
    redis_conn.ping()
    logger.info("✅ Conexão com Redis estabelecida com sucesso")
except Exception as e:
    logger.error(f"❌ Erro ao conectar ao Redis: {e}")
    sys.exit(1)

# Configurar filas
queues = [
    Queue("bot_tasks", connection=redis_conn)
]

def signal_handler(sig, frame):
    """Handler para sinais de parada"""
    logger.info("🛑 Recebido sinal de parada. Encerrando worker...")
    sys.exit(0)

# Configurar handlers de sinais
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def start_worker():
    """Inicia o worker RQ para processar tarefas da fila bot_tasks"""
    try:
        logger.info("⚙️ Iniciando worker para escutar fila: bot_tasks")
        worker = Worker(queues, connection=redis_conn)
        worker.work()
    except Exception as e:
        logger.error(f"❌ Erro ao iniciar worker: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logger.info("🚀 Iniciando worker RQ para processamento de tarefas")
    start_worker()
