import json
import subprocess
import os
import sys
import time
import threading
import logging
from datetime import timedelta
from typing import Dict, Any, Optional
from pathlib import Path
import redis
from rq import Queue
from rq.job import Job
from rq.exceptions import NoSuchJobError
import shared

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tasks.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configurações Redis com pool otimizado
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", 50))
REDIS_SOCKET_TIMEOUT = int(os.environ.get("REDIS_SOCKET_TIMEOUT", 10))
REDIS_SOCKET_CONNECT_TIMEOUT = int(os.environ.get("REDIS_SOCKET_CONNECT_TIMEOUT", 5))

# Configurações de performance
JOB_TIMEOUT = int(os.environ.get("JOB_TIMEOUT", 300))  # 5 minutos
JOB_RESULT_TTL = int(os.environ.get("JOB_RESULT_TTL", 3600))  # 1 hora
FAILED_JOB_TTL = int(os.environ.get("FAILED_JOB_TTL", 86400))  # 24 horas
MAX_RETRIES = int(os.environ.get("MAX_TASK_RETRIES", 3))
RETRY_DELAYS = [60, 300, 900]  # 1min, 5min, 15min

# Pool de conexões Redis com configurações otimizadas
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    max_connections=REDIS_MAX_CONNECTIONS,
    socket_timeout=REDIS_SOCKET_TIMEOUT,
    socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
    retry_on_timeout=True,
    health_check_interval=30,
    socket_keepalive=True,
    socket_keepalive_options={}
)

# Cliente Redis configurado
redis_client = redis.StrictRedis(
    connection_pool=redis_pool,
    decode_responses=False
)

# Configurar fila RQ com configurações otimizadas
if not hasattr(shared, 'rq_queue') or shared.rq_queue is None:
    shared.rq_queue = Queue(
        "bot_tasks",
        connection=redis_client,
        default_timeout=JOB_TIMEOUT,
        result_ttl=JOB_RESULT_TTL,
        failure_ttl=FAILED_JOB_TTL
    )

# Filas adicionais para prioridade
high_priority_queue = Queue("high_priority", connection=redis_client, default_timeout=JOB_TIMEOUT)
low_priority_queue = Queue("low_priority", connection=redis_client, default_timeout=JOB_TIMEOUT)

# Lock para operações thread-safe
_lock = threading.Lock()

# Garantir que variáveis compartilhadas estejam definidas
if not hasattr(shared, 'active_campaigns'):
    shared.active_campaigns = {}
if not hasattr(shared, 'user_clients'):
    shared.user_clients = {}
if not hasattr(shared, 'user_settings'):
    shared.user_settings = {}
if not hasattr(shared, 'user_group_list'):
    shared.user_group_list = {}
if not hasattr(shared, 'statistics'):
    shared.statistics = {"messages_sent": 0, "active_campaigns": 0}

# Cache para msgpack com fallback para JSON
try:
    import msgpack
    HAS_MSGPACK = True
    logger.info("msgpack disponível - usando serialização binária otimizada")
except ImportError:
    msgpack = None
    HAS_MSGPACK = False
    logger.warning("msgpack não encontrado - usando JSON como fallback")

def get_redis_connection() -> redis.StrictRedis:
    """Obtém conexão Redis com retry automático"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Testar conexão
            redis_client.ping()
            return redis_client
        except redis.ConnectionError as e:
            if attempt == max_retries - 1:
                logger.error(f"Falha ao conectar Redis após {max_retries} tentativas: {e}")
                raise
            logger.warning(f"Erro de conexão Redis (tentativa {attempt + 1}): {e}")
            time.sleep(2 ** attempt)  # Backoff exponencial

def serialize_data(data: Dict[str, Any]) -> bytes:
    """Serializa dados usando msgpack ou JSON"""
    try:
        if HAS_MSGPACK:
            return msgpack.packb(data, use_bin_type=True)
        else:
            import json
            return json.dumps(data).encode('utf-8')
    except Exception as e:
        logger.error(f"Erro ao serializar dados: {e}")
        # Fallback para JSON simples
        import json
        return json.dumps(str(data)).encode('utf-8')

def deserialize_data(data: bytes) -> Dict[str, Any]:
    """Deserializa dados usando msgpack ou JSON"""
    try:
        if HAS_MSGPACK:
            return msgpack.unpackb(data, raw=False)
        else:
            import json
            return json.loads(data.decode('utf-8'))
    except Exception as e:
        logger.error(f"Erro ao deserializar dados: {e}")
        return {}

def load_cache(user_id: int) -> Dict[str, Any]:
    """Carrega cache do usuário com tratamento de erro melhorado"""
    key = f"usercache:{user_id}"
    try:
        redis_conn = get_redis_connection()
        packed = redis_conn.get(key)
        if packed is not None:
            return deserialize_data(packed)
        logger.debug(f"Cache não encontrado para usuário {user_id}")
        return {}
    except Exception as e:
        logger.error(f"Erro ao carregar cache para user_id {user_id}: {str(e)}")
        return {}

def save_cache(user_id: int, data: Dict[str, Any], ttl: int = 3600) -> bool:
    """Salva cache com TTL configurável e retry"""
    key = f"usercache:{user_id}"
    try:
        redis_conn = get_redis_connection()
        packed = serialize_data(data)
        redis_conn.setex(key, ttl, packed)
        logger.debug(f"Cache salvo para usuário {user_id} com TTL {ttl}s")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar cache para user_id {user_id}: {str(e)}")
        return False

def delete_cache(user_id: int) -> bool:
    """Remove cache do usuário"""
    key = f"usercache:{user_id}"
    try:
        redis_conn = get_redis_connection()
        redis_conn.delete(key)
        logger.debug(f"Cache removido para usuário {user_id}")
        return True
    except Exception as e:
        logger.error(f"Erro ao remover cache para user_id {user_id}: {str(e)}")
        return False

def execute_async_helper(user_id: int, link: str, timeout: int = JOB_TIMEOUT) -> Dict[str, Any]:
    """Executa o script async_helper.py com timeout e tratamento de erro robusto"""
    try:
        # Verificar se arquivo exists
        script_path = Path("async_helper.py")
        if not script_path.exists():
            logger.error("async_helper.py não encontrado")
            return {"status": "error", "message": "Script auxiliar não encontrado"}

        # Preparar comando
        cmd = [sys.executable, str(script_path), str(user_id), link]

        logger.info(f"Executando script auxiliar para usuário {user_id} com link {link}")

        # Executar com timeout
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
            cwd=os.getcwd(),
            env=os.environ.copy()
        )

        # Verificar se processo executou com sucesso
        if process.returncode != 0:
            error_msg = process.stderr.strip() if process.stderr else "Erro desconhecido"
            logger.error(f"Script auxiliar falhou para usuário {user_id}: {error_msg}")
            return {
                "status": "error",
                "message": f"Falha na execução: {error_msg}",
                "return_code": process.returncode
            }

        # Parse da saída
        output = process.stdout.strip()
        if not output:
            logger.warning(f"Script auxiliar retornou saída vazia para usuário {user_id}")
            return {"status": "error", "message": "Saída vazia do script auxiliar"}

        try:
            result = json.loads(output)
            logger.info(f"Script auxiliar executado com sucesso para usuário {user_id}")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Erro ao decodificar JSON do script auxiliar para usuário {user_id}: {e}")
            logger.debug(f"Saída bruta: {output}")
            return {
                "status": "error",
                "message": "Resposta inválida do script auxiliar",
                "raw_output": output
            }
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout ({timeout}s) ao executar script auxiliar para usuário {user_id}")
        shared.update_user_statistics(user_id, 'failed_forwards')
        return {"status": "error", "message": f"Timeout após {timeout} segundos"}
    except FileNotFoundError:
        logger.error("Python interpreter não encontrado")
        return {"status": "error", "message": "Interpretador Python não encontrado"}
    except Exception as e:
        logger.error(f"Erro inesperado ao executar script auxiliar para usuário {user_id}: {str(e)}")
        shared.update_user_statistics(user_id, 'failed_forwards')
        return {"status": "error", "message": f"Erro inesperado: {str(e)}"}

def should_retry_task(result: Dict[str, Any], attempt: int) -> bool:
    """Determina se uma tarefa deve ser reexecutada"""
    if attempt >= MAX_RETRIES:
        return False

    # Sempre retry para erros de timeout ou conexão
    error_msg = result.get("message", "").lower()
    retry_conditions = [
        "timeout" in error_msg,
        "connection" in error_msg,
        "flood" in error_msg,
        "network" in error_msg,
        "temporary" in error_msg
    ]
    return any(retry_conditions)

def forward_message_com_RQ(user_id: int, attempt: int = 1) -> Dict[str, Any]:
    """Função principal para encaminhamento com otimizações e retry automático"""
    try:
        # Verificar rate limiting
        if shared.is_rate_limited(user_id):
            logger.warning(f"Rate limit atingido para usuário {user_id}")
            return {"status": "error", "message": "Rate limit atingido"}

        # Carregar dados do cache
        cache_data = load_cache(user_id)
        link = cache_data.get("msg_link")

        if not link:
            logger.warning(f"Link de mensagem não encontrado no cache para usuário {user_id}")
            return {"status": "error", "message": "Link de mensagem não encontrado"}

        logger.info(f"Iniciando encaminhamento para usuário {user_id} (tentativa {attempt}) com link {link}")

        # Executar script auxiliar
        result = execute_async_helper(user_id, link)

        # Atualizar estatísticas baseado no resultado
        if result.get("status") == "success":
            shared.update_user_statistics(user_id, 'successful_forwards')
            logger.info(f"Encaminhamento bem-sucedido para usuário {user_id}")
        else:
            shared.update_user_statistics(user_id, 'failed_forwards')
            logger.warning(f"Encaminhamento falhou para usuário {user_id}: {result.get('message')}")

        # Verificar se deve fazer retry
        if should_retry_task(result, attempt):
            delay = RETRY_DELAYS[min(attempt - 1, len(RETRY_DELAYS) - 1)]
            logger.info(f"Agendando retry {attempt + 1} para usuário {user_id} em {delay}s")
            # Agendar retry
            shared.rq_queue.enqueue_in(
                timedelta(seconds=delay),
                forward_message_com_RQ,
                user_id,
                attempt + 1,
                job_timeout=JOB_TIMEOUT,
                result_ttl=JOB_RESULT_TTL
            )

        # Reenfileirar próxima execução se campanha ainda ativa
        interval = cache_data.get("interval")
        if interval and user_id in shared.active_campaigns:
            schedule_next_execution(user_id, interval)
        else:
            logger.info(f"Campanha não está mais ativa para usuário {user_id}")

        return result

    except Exception as e:
        logger.error(f"Erro geral na função forward_message_com_RQ para usuário {user_id}: {str(e)}", exc_info=True)
        shared.update_user_statistics(user_id, 'failed_forwards')
        return {"status": "error", "message": f"Erro interno: {str(e)}"}

def schedule_next_execution(user_id: int, interval: int) -> Optional[Job]:
    """Agenda próxima execução da tarefa"""
    try:
        interval_minutes = int(interval)
        # Adicionar jitter para distribuir carga
        import random
        jitter_seconds = random.randint(0, min(30, interval_minutes * 6))  # Até 10% do intervalo ou 30s
        delay = timedelta(minutes=interval_minutes, seconds=jitter_seconds)
        # Enfileirar próxima tarefa
        job = shared.rq_queue.enqueue_in(
            delay,
            forward_message_com_RQ,
            user_id,
            1,  # Reset attempt counter
            job_timeout=JOB_TIMEOUT,
            result_ttl=JOB_RESULT_TTL
        )
        if job:
            # Atualizar job_id na campanha ativa
            with _lock:
                if user_id in shared.active_campaigns:
                    shared.active_campaigns[user_id]['job_id'] = job.id
            logger.info(f"Próxima execução agendada para usuário {user_id} em {delay.total_seconds():.0f}s")
            return job
        else:
            logger.error(f"Falha ao agendar próxima execução para usuário {user_id}")
    except Exception as e:
        logger.error(f"Erro ao agendar próxima execução para usuário {user_id}: {str(e)}")
    return None

def cleanup_expired_jobs() -> Dict[str, int]:
    """Remove jobs expirados das filas"""
    try:
        cleaned = {"failed": 0, "finished": 0, "scheduled": 0}
        # Limpar jobs falhados antigos (mais de 24h)
        failed_registry = shared.rq_queue.failed_job_registry
        before_failed = len(failed_registry)
        failed_registry.cleanup(timestamp=time.time() - FAILED_JOB_TTL)
        cleaned["failed"] = before_failed - len(failed_registry)
        # Limpar jobs finalizados antigos (mais de 1h)
        finished_registry = shared.rq_queue.finished_job_registry
        before_finished = len(finished_registry)
        finished_registry.cleanup(timestamp=time.time() - JOB_RESULT_TTL)
        cleaned["finished"] = before_finished - len(finished_registry)
        # Limpar jobs agendados órfãos
        scheduled_registry = shared.rq_queue.scheduled_job_registry
        before_scheduled = len(scheduled_registry)
        # Remover jobs de usuários que não têm mais campanha ativa
        orphaned_jobs = []
        for job_id in scheduled_registry.get_job_ids():
            try:
                job = Job.fetch(job_id, connection=redis_client)
                if job.args and len(job.args) > 0:
                    job_user_id = job.args[0]
                    if job_user_id not in shared.active_campaigns:
                        orphaned_jobs.append(job_id)
            except (NoSuchJobError, AttributeError, IndexError):
                orphaned_jobs.append(job_id)
        for job_id in orphaned_jobs:
            try:
                scheduled_registry.remove(job_id)
            except Exception:
                pass
        cleaned["scheduled"] = len(orphaned_jobs)
        if any(cleaned.values()):
            logger.info(f"Limpeza concluída: {cleaned}")
        return cleaned
    except Exception as e:
        logger.error(f"Erro na limpeza de jobs: {e}")
        return {"failed": 0, "finished": 0, "scheduled": 0}

def get_queue_statistics() -> Dict[str, Any]:
    """Retorna estatísticas detalhadas das filas"""
    try:
        return {
            "main_queue": {
                "pending": len(shared.rq_queue),
                "failed": len(shared.rq_queue.failed_job_registry),
                "finished": len(shared.rq_queue.finished_job_registry),
                "scheduled": len(shared.rq_queue.scheduled_job_registry),
                "started": len(shared.rq_queue.started_job_registry)
            },
            "high_priority": {
                "pending": len(high_priority_queue),
                "failed": len(high_priority_queue.failed_job_registry),
                "finished": len(high_priority_queue.finished_job_registry)
            },
            "low_priority": {
                "pending": len(low_priority_queue),
                "failed": len(low_priority_queue.failed_job_registry),
                "finished": len(low_priority_queue.finished_job_registry)
            },
            "redis_info": {
                "used_memory": redis_client.info()["used_memory_human"],
                "connected_clients": redis_client.info()["connected_clients"],
                "total_commands_processed": redis_client.info()["total_commands_processed"]
            }
        }
    except Exception as e:
        logger.error(f"Erro ao obter estatísticas da fila: {e}")
        return {"error": str(e)}

def cancel_user_tasks(user_id: int) -> int:
    """Cancela todas as tarefas de um usuário específico"""
    canceled = 0
    try:
        # Cancelar jobs agendados
        scheduled_registry = shared.rq_queue.scheduled_job_registry
        for job_id in list(scheduled_registry.get_job_ids()):
            try:
                job = Job.fetch(job_id, connection=redis_client)
                if job.args and len(job.args) > 0 and job.args[0] == user_id:
                    job.cancel()
                    shared.rq_queue.remove(job_id)
                    canceled += 1
                    logger.debug(f"Job {job_id} cancelado para usuário {user_id}")
            except (NoSuchJobError, AttributeError):
                continue
        # Cancelar jobs pendentes
        for job in shared.rq_queue.jobs:
            try:
                if job.args and len(job.args) > 0 and job.args[0] == user_id:
                    job.cancel()
                    canceled += 1
                    logger.debug(f"Job pendente cancelado para usuário {user_id}")
            except Exception:
                continue
        if canceled > 0:
            logger.info(f"Cancelados {canceled} jobs para usuário {user_id}")
        return canceled
    except Exception as e:
        logger.error(f"Erro ao cancelar jobs para usuário {user_id}: {e}")
        return 0

# Executar limpeza periódica em thread separada
def periodic_cleanup_worker():
    """Worker para limpeza periódica executado em thread separada"""
    while True:
        try:
            # Limpeza de jobs
            cleanup_expired_jobs()
            # Limpeza de dados compartilhados
            shared.cleanup_expired_data()
            # Aguardar próxima execução (30 minutos)
            time.sleep(1800)
        except Exception as e:
            logger.error(f"Erro na limpeza periódica: {e}")
            time.sleep(60)  # Retry em 1 minuto

# Iniciar thread de limpeza
cleanup_thread = threading.Thread(target=periodic_cleanup_worker, daemon=True)
cleanup_thread.start()

logger.info("✅ Sistema de tasks inicializado com otimizações para alta concorrência")
logger.info(f"📊 Configurações: timeout={JOB_TIMEOUT}s, result_ttl={JOB_RESULT_TTL}s, max_retries={MAX_RETRIES}")

