import asyncio
import time
import threading
from collections import defaultdict
from typing import Dict, List, Any, Optional

# Configurações do banco
AsyncSessionLocal = None
User = None

# Redis e RQ
rq_queue = None

# Cache e estado da aplicação com thread safety
_lock = threading.Lock()

# Estados dos usuários
active_campaigns: Dict[int, Dict[str, Any]] = {}
user_clients: Dict[int, Any] = {}
user_settings: Dict[int, Dict[str, Any]] = defaultdict(dict)
user_group_list: Dict[int, List[Any]] = defaultdict(list)

# Estatísticas globais
statistics = {
    "messages_sent": 0, 
    "active_campaigns": 0,
    "total_users": 0,
    "successful_forwards": 0,
    "failed_forwards": 0,
    "total_groups_loaded": 0,
    "authentication_attempts": 0,
    "successful_authentications": 0,
    "payment_attempts": 0,
    "successful_payments": 0
}

# Métricas Prometheus
messages_sent_counter = None
active_campaigns_gauge = None
message_forward_time = None
user_operations_histogram = None
error_counter = None

# Rate limiting configuration
user_rate_limits = defaultdict(list)
RATE_LIMIT_WINDOW = 60  # 1 minuto
RATE_LIMIT_MAX_REQUESTS = 10  # máximo 10 requests por minuto por usuário

# Connection pooling
connection_pool = None
redis_pool = None

# Cache TTL settings (em segundos)
CACHE_TTL = {
    'user_groups': 3600,        # 1 hora
    'admin_status': 1800,       # 30 minutos
    'user_data': 7200,          # 2 horas
    'payment_status': 900,      # 15 minutos
    'session_validation': 300   # 5 minutos
}

# Configurações de performance
PERFORMANCE_CONFIG = {
    'max_concurrent_operations': 10,
    'session_check_interval': 300,
    'flood_limit': 5,
    'flood_interval': 10,
    'client_timeout': 30,
    'max_groups_per_batch': 10,
    'max_forwards_per_batch': 5
}

def is_rate_limited(user_id: int) -> bool:
    """
    Verifica se usuário está sendo rate limited
    Thread-safe implementation
    """
    with _lock:
        now = time.time()
        
        # Limpar requests antigos
        user_rate_limits[user_id] = [
            req_time for req_time in user_rate_limits[user_id] 
            if now - req_time < RATE_LIMIT_WINDOW
        ]
        
        # Verificar limite
        if len(user_rate_limits[user_id]) >= RATE_LIMIT_MAX_REQUESTS:
            return True
        
        # Adicionar request atual
        user_rate_limits[user_id].append(now)
        return False

def update_statistics(key: str, value: int = 1) -> None:
    """
    Atualiza estatísticas de forma thread-safe
    """
    with _lock:
        statistics[key] = statistics.get(key, 0) + value
        
        # Atualizar métricas Prometheus se disponíveis
        if key == "messages_sent" and messages_sent_counter:
            try:
                messages_sent_counter.inc(value)
            except Exception:
                pass
        elif key == "active_campaigns" and active_campaigns_gauge:
            try:
                active_campaigns_gauge.set(len(active_campaigns))
            except Exception:
                pass
        elif key == "failed_forwards" and error_counter:
            try:
                error_counter.labels(error_type='forward_failure').inc(value)
            except Exception:
                pass

def get_user_statistics(user_id: int) -> Dict[str, int]:
    """
    Retorna estatísticas específicas do usuário
    """
    with _lock:
        user_key = f'user_{user_id}'
        return {
            'messages_sent': statistics.get(f'{user_key}_messages_sent', 0),
            'successful_forwards': statistics.get(f'{user_key}_successful_forwards', 0),
            'failed_forwards': statistics.get(f'{user_key}_failed_forwards', 0),
            'groups_loaded': statistics.get(f'{user_key}_groups_loaded', 0),
            'last_activity': statistics.get(f'{user_key}_last_activity', 0)
        }

def update_user_statistics(user_id: int, key: str, value: int = 1) -> None:
    """
    Atualiza estatísticas específicas do usuário
    """
    user_key = f'user_{user_id}_{key}'
    update_statistics(user_key, value)
    
    # Atualizar timestamp de última atividade
    statistics[f'user_{user_id}_last_activity'] = int(time.time())

def cleanup_expired_data() -> None:
    """
    Remove dados expirados dos caches
    Thread-safe implementation
    """
    with _lock:
        current_time = time.time()
        
        # Limpar rate limits antigos
        expired_users = []
        for user_id in list(user_rate_limits.keys()):
            user_rate_limits[user_id] = [
                req_time for req_time in user_rate_limits[user_id]
                if current_time - req_time < RATE_LIMIT_WINDOW
            ]
            if not user_rate_limits[user_id]:
                expired_users.append(user_id)
        
        for user_id in expired_users:
            del user_rate_limits[user_id]

def add_active_campaign(user_id: int, campaign_data: Dict[str, Any]) -> None:
    """
    Adiciona campanha ativa de forma thread-safe
    """
    with _lock:
        active_campaigns[user_id] = campaign_data
        update_statistics('active_campaigns', 0)  # Força atualização do gauge

def remove_active_campaign(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Remove campanha ativa de forma thread-safe
    """
    with _lock:
        campaign = active_campaigns.pop(user_id, None)
        update_statistics('active_campaigns', 0)  # Força atualização do gauge
        return campaign

def get_active_campaign(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Obtém campanha ativa de forma thread-safe
    """
    with _lock:
        return active_campaigns.get(user_id)

def has_active_campaign(user_id: int) -> bool:
    """
    Verifica se usuário tem campanha ativa
    """
    with _lock:
        return (
            user_id in active_campaigns and
            active_campaigns[user_id].get('job_id') is not None
        )

def get_system_status() -> Dict[str, Any]:
    """
    Retorna status completo do sistema
    """
    with _lock:
        return {
            'statistics': statistics.copy(),
            'active_campaigns_count': len(active_campaigns),
            'connected_users': len(user_clients),
            'cached_users': len(user_settings),
            'rate_limited_users': len([
                user_id for user_id, timestamps in user_rate_limits.items()
                if len(timestamps) >= RATE_LIMIT_MAX_REQUESTS
            ]),
            'performance_config': PERFORMANCE_CONFIG.copy(),
            'cache_ttl': CACHE_TTL.copy()
        }

def cleanup_user_data(user_id: int) -> None:
    """
    Limpa todos os dados do usuário da memória
    """
    with _lock:
        # Remover campanha ativa
        active_campaigns.pop(user_id, None)
        
        # Remover configurações
        user_settings.pop(user_id, None)
        
        # Remover lista de grupos
        user_group_list.pop(user_id, None)
        
        # Remover rate limiting
        user_rate_limits.pop(user_id, None)
        
        # Remover cliente se existir
        client = user_clients.pop(user_id, None)
        if client:
            try:
                # Tentar desconectar de forma assíncrona se possível
                if hasattr(client, 'disconnect'):
                    asyncio.create_task(client.disconnect())
            except Exception:
                pass

def get_performance_metrics() -> Dict[str, Any]:
    """
    Retorna métricas de performance do sistema
    """
    import psutil
    import gc
    
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        
        return {
            'memory_usage_mb': memory_info.rss / 1024 / 1024,
            'cpu_percent': process.cpu_percent(),
            'open_files': len(process.open_files()),
            'threads_count': process.num_threads(),
            'gc_collected': gc.collect(),
            'active_campaigns': len(active_campaigns),
            'connected_clients': len(user_clients),
            'cached_users': len(user_settings)
        }
    except Exception:
        return {
            'error': 'Could not collect performance metrics',
            'active_campaigns': len(active_campaigns),
            'connected_clients': len(user_clients),
            'cached_users': len(user_settings)
        }

# Auto-cleanup thread
def _auto_cleanup_thread():
    """
    Thread para limpeza automática de dados expirados
    """
    while True:
        try:
            cleanup_expired_data()
            time.sleep(300)  # A cada 5 minutos
        except Exception as e:
            print(f"Erro na limpeza automática: {e}")
            time.sleep(60)  # Tentar novamente em 1 minuto

# Iniciar thread de limpeza automática
_cleanup_thread = threading.Thread(target=_auto_cleanup_thread, daemon=True)
_cleanup_thread.start()

# Funções de compatibilidade (para manter API antiga)
def get_user_setting(user_id: int, key: str, default=None):
    """Obtém configuração do usuário"""
    with _lock:
        return user_settings.get(user_id, {}).get(key, default)

def set_user_setting(user_id: int, key: str, value):
    """Define configuração do usuário"""
    with _lock:
        if user_id not in user_settings:
            user_settings[user_id] = {}
        user_settings[user_id][key] = value

def get_user_groups(user_id: int) -> List[Any]:
    """Obtém lista de grupos do usuário"""
    with _lock:
        return user_group_list.get(user_id, [])

def set_user_groups(user_id: int, groups: List[Any]):
    """Define lista de grupos do usuário"""
    with _lock:
        user_group_list[user_id] = groups
        update_user_statistics(user_id, 'groups_loaded', len(groups))

def get_user_client(user_id: int):
    """Obtém cliente do usuário"""
    with _lock:
        return user_clients.get(user_id)

def set_user_client(user_id: int, client):
    """Define cliente do usuário"""
    with _lock:
        user_clients[user_id] = client

def remove_user_client(user_id: int):
    """Remove cliente do usuário"""
    with _lock:
        return user_clients.pop(user_id, None)

# Configurações padrão para logs
LOG_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'detailed': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
        'simple': {
            'format': '%(levelname)s - %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',
            'formatter': 'simple',
            'stream': 'ext://sys.stdout'
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG',
            'formatter': 'detailed',
            'filename': 'bot.log',
            'maxBytes': 10485760,  # 10MB
            'backupCount': 5
        }
    },
    'loggers': {
        '': {
            'handlers': ['console', 'file'],
            'level': 'DEBUG',
            'propagate': False
        }
    }
}

print("✅ Módulo shared.py carregado com otimizações para alta concorrência")

