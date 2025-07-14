import asyncio
import sys
import json
import os
import time
import logging
from typing import Optional, List, Tuple, Dict, Any
from dotenv import load_dotenv
from sqlalchemy import select, Column, Integer, BigInteger, String, Boolean, DateTime, Index
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.sql import func
from telethon import TelegramClient
from telethon.errors import (
    FloodWaitError, PhoneNumberBannedError, MessageIdInvalidError,
    ChatAdminRequiredError, UserBannedInChannelError, PeerIdInvalidError
)
from telethon.tl.types import ChannelParticipantsAdmins
import shared

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('async_helper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

# Configurações do banco
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./meubanco.db")

# Configurações de performance
MAX_CONCURRENT_FORWARDS = int(os.environ.get("MAX_CONCURRENT_FORWARDS", 3))
MAX_CONCURRENT_GROUPS = int(os.environ.get("MAX_CONCURRENT_GROUPS", 5))
CLIENT_CACHE_TTL = int(os.environ.get("CLIENT_CACHE_TTL", 3600))
RETRY_DELAYS = [1, 2, 5, 10, 30]  # Delays progressivos para retry

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, unique=True, nullable=False, index=True)
    phone = Column(String, nullable=True)
    api_id = Column(String, nullable=True)
    api_hash = Column(String, nullable=True)
    session_path = Column(String, nullable=True)
    is_authenticated = Column(Boolean, default=False, index=True)
    is_banned = Column(Boolean, default=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        Index('idx_user_auth_status', 'telegram_user_id', 'is_authenticated'),
        Index('idx_user_ban_status', 'telegram_user_id', 'is_banned'),
    )

class UserAdminGroup(Base):
    __tablename__ = "user_admin_groups"
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, nullable=False, index=True)
    group_id = Column(BigInteger, nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

# Pool de conexões otimizado
engine_config = {
    "echo": False,
    "future": True,
    "pool_pre_ping": True,
    "pool_recycle": 3600,
}

if "postgresql" in DATABASE_URL:
    engine_config.update({
        "pool_size": 20,
        "max_overflow": 30,
        "pool_timeout": 30,
    })
else:
    engine_config.update({
        "pool_size": 10,
        "max_overflow": 20,
        "pool_timeout": 20,
    })

engine = create_async_engine(DATABASE_URL, **engine_config)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

shared.AsyncSessionLocal = AsyncSessionLocal
shared.User = User

# Cache de clientes com TTL e cleanup automático
client_cache: Dict[int, Dict[str, Any]] = {}
cache_lock = asyncio.Lock()

async def get_user_client(telegram_user_id: int, session: AsyncSession) -> Optional[TelegramClient]:
    """Obtém cliente Telethon com cache otimizado e gestão de conexões"""
    async with cache_lock:
        try:
            current_time = time.time()
            # Verificar cache primeiro
            if telegram_user_id in client_cache:
                client_data = client_cache[telegram_user_id]
                # Verificar TTL do cache
                if current_time - client_data['timestamp'] < CLIENT_CACHE_TTL:
                    client = client_data['client']
                    # Verificar se cliente ainda está válido
                    if client.is_connected():
                        try:
                            if await asyncio.wait_for(client.is_user_authorized(), timeout=5):
                                logger.debug(f"Cliente do cache válido para usuário {telegram_user_id}")
                                return client
                        except asyncio.TimeoutError:
                            logger.warning(f"Timeout ao verificar autorização do cliente para {telegram_user_id}")
                    # Cliente inválido, remover do cache
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    del client_cache[telegram_user_id]

            # Buscar usuário no banco com timeout
            stmt = select(User).where(
                User.telegram_user_id == telegram_user_id,
                User.is_authenticated == True
            )
            result = await asyncio.wait_for(session.execute(stmt), timeout=10)
            user = result.scalar_one_or_none()

            if not user:
                logger.warning(f"Usuário {telegram_user_id} não encontrado ou não autenticado no banco")
                return None

            # Verificar se arquivo de sessão existe
            if not user.session_path or not os.path.exists(user.session_path):
                logger.warning(f"Arquivo de sessão não encontrado para usuário {telegram_user_id}")
                return None

            # Criar novo cliente
            client = TelegramClient(user.session_path, user.api_id, user.api_hash)
            # Conectar com timeout
            await asyncio.wait_for(client.connect(), timeout=15)
            # Verificar autorização
            if not await asyncio.wait_for(client.is_user_authorized(), timeout=10):
                await client.disconnect()
                logger.warning(f"Cliente não autorizado para usuário {telegram_user_id}")
                return None

            # Adicionar ao cache
            client_cache[telegram_user_id] = {
                'client': client,
                'timestamp': current_time
            }
            logger.info(f"Cliente criado e cacheado para usuário {telegram_user_id}")
            return client

        except asyncio.TimeoutError:
            logger.error(f"Timeout ao criar cliente para usuário {telegram_user_id}")
            return None
        except Exception as e:
            logger.error(f"Erro ao criar cliente Telethon para usuário {telegram_user_id}: {str(e)}")
            return None

async def preload_groups_for_user(telegram_user_id: int, client: TelegramClient) -> Tuple[List[Any], List[int]]:
    """Carrega grupos do usuário com otimização e controle de rate limiting"""
    groups = []
    admin_group_ids = []
    try:
        # Obter informações do usuário
        me = await asyncio.wait_for(client.get_me(), timeout=10)
        # Semáforo para controlar requisições simultâneas
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_GROUPS)

        async def process_dialog(dialog):
            """Processa um diálogo específico"""
            async with semaphore:
                try:
                    if dialog.is_group and not dialog.archived and hasattr(dialog.entity, 'id'):
                        groups.append(dialog.entity)
                        # Verificar status de admin com retry
                        is_admin = await check_admin_with_retry(client, dialog.entity, me.id)
                        if is_admin:
                            admin_group_ids.append(dialog.entity.id)
                            logger.debug(f"Usuário {telegram_user_id} é admin no grupo {dialog.entity.id}")
                        # Pequena pausa para evitar rate limiting
                        await asyncio.sleep(0.1)
                except Exception as e:
                    logger.warning(f"Erro ao processar grupo para usuário {telegram_user_id}: {e}")

        # Processar diálogos em lotes
        tasks = []
        dialog_count = 0
        async for dialog in client.iter_dialogs():
            if dialog.is_group and not dialog.archived:
                tasks.append(process_dialog(dialog))
                dialog_count += 1
                # Processar em lotes de 10 para controlar memória
                if len(tasks) >= 10:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []
                # Pausa entre lotes para rate limiting
                await asyncio.sleep(0.5)
            # Limite máximo de grupos processados
            if dialog_count >= 500:  # Limite para evitar sobrecarga
                logger.warning(f"Limite de grupos atingido para usuário {telegram_user_id}")
                break
        # Processar tarefas restantes
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Salvar grupos admin no banco
        await save_admin_groups_to_db(telegram_user_id, admin_group_ids)
        # Atualizar cache compartilhado
        shared.set_user_groups(telegram_user_id, groups)
        logger.info(
            f"Grupos carregados para usuário {telegram_user_id}: "
            f"{len(groups)} total, {len(admin_group_ids)} admin"
        )
        return groups, admin_group_ids
    except Exception as e:
        logger.error(f"Erro ao carregar grupos para usuário {telegram_user_id}: {str(e)}")
        return [], []

async def check_admin_with_retry(client: TelegramClient, group, user_id: int, max_retries: int = 3) -> bool:
    """Verifica se usuário é admin com retry automático"""
    for attempt in range(max_retries):
        try:
            participants = await asyncio.wait_for(
                client.get_participants(group, filter=ChannelParticipantsAdmins),
                timeout=15
            )
            admin_ids = {p.id for p in participants}
            return user_id in admin_ids
        except asyncio.TimeoutError:
            logger.warning(f"Timeout ao verificar admin (tentativa {attempt + 1})")
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAYS[attempt])
            continue
        except FloodWaitError as e:
            logger.warning(f"FloodWait ao verificar admin: {e.seconds}s")
            if attempt < max_retries - 1:
                await asyncio.sleep(min(e.seconds, 30))
            continue
        except Exception as e:
            logger.warning(f"Erro ao verificar admin (tentativa {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(RETRY_DELAYS[attempt])
            continue
    return False

async def save_admin_groups_to_db(telegram_user_id: int, admin_group_ids: List[int]) -> None:
    """Salva grupos admin no banco de dados"""
    try:
        async with AsyncSessionLocal() as session:
            # Remover grupos admin antigos
            from sqlalchemy import delete
            stmt = delete(UserAdminGroup).where(UserAdminGroup.telegram_user_id == telegram_user_id)
            await session.execute(stmt)
            # Adicionar novos grupos admin
            for group_id in admin_group_ids:
                admin_group = UserAdminGroup(
                    telegram_user_id=telegram_user_id,
                    group_id=group_id
                )
                session.add(admin_group)
            await session.commit()
            logger.debug(f"Salvos {len(admin_group_ids)} grupos admin para usuário {telegram_user_id}")
    except Exception as e:
        logger.error(f"Erro ao salvar grupos admin no banco: {e}")

async def get_admin_group_ids(telegram_user_id: int) -> List[int]:
    """Obtém IDs dos grupos onde o usuário é admin"""
    try:
        async with AsyncSessionLocal() as session:
            stmt = select(UserAdminGroup.group_id).where(
                UserAdminGroup.telegram_user_id == telegram_user_id
            )
            result = await session.execute(stmt)
            group_ids = [row[0] for row in result.fetchall()]
            return group_ids
    except Exception as e:
        logger.error(f"Erro ao buscar grupos admin para usuário {telegram_user_id}: {e}")
        return []

async def forward_message_batch(client: TelegramClient, groups: List[Any], message: Any, user_id: int) -> Tuple[int, int]:
    """Encaminha mensagem para grupos em lotes com controle de concorrência"""
    successful = 0
    failed = 0
    # Semáforo para controlar forwards simultâneos
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FORWARDS)

    async def forward_to_single_group(group):
        """Encaminha mensagem para um grupo específico"""
        async with semaphore:
            try:
                await asyncio.wait_for(
                    client.forward_messages(group, message),
                    timeout=30
                )
                logger.debug(f"Mensagem encaminhada com sucesso para grupo {group.id}")
                return True
            except FloodWaitError as e:
                logger.warning(f"FloodWait ao encaminhar para grupo {group.id}: {e.seconds}s")
                return False
            except UserBannedInChannelError:
                logger.warning(f"Usuário banido no grupo {group.id}")
                return False
            except ChatAdminRequiredError:
                logger.warning(f"Permissão de admin necessária no grupo {group.id}")
                return False
            except PeerIdInvalidError:
                logger.warning(f"ID do grupo inválido: {group.id}")
                return False
            except asyncio.TimeoutError:
                logger.warning(f"Timeout ao encaminhar para grupo {group.id}")
                return False
            except Exception as e:
                logger.error(f"Erro ao encaminhar para grupo {group.id}: {e}")
                return False

    # Processar grupos em lotes
    batch_size = 10
    for i in range(0, len(groups), batch_size):
        batch = groups[i:i + batch_size]
        # Executar forwards do lote
        tasks = [forward_to_single_group(group) for group in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Contar sucessos e falhas
        for result in results:
            if result is True:
                successful += 1
            else:
                failed += 1
        # Pausa entre lotes para evitar rate limiting
        if i + batch_size < len(groups):
            await asyncio.sleep(1)

    # Atualizar estatísticas do usuário
    shared.update_user_statistics(user_id, 'successful_forwards', successful)
    shared.update_user_statistics(user_id, 'failed_forwards', failed)
    return successful, failed

async def forward_message(user_id: int, link: str) -> Dict[str, Any]:
    """Função principal para encaminhar mensagens com otimizações e tratamento de erros"""
    result = {"status": "error", "message": "Unknown error"}
    start_time = time.time()
    try:
        async with AsyncSessionLocal() as session:
            # Buscar usuário
            stmt = select(User).where(User.telegram_user_id == user_id)
            result_query = await asyncio.wait_for(session.execute(stmt), timeout=10)
            user = result_query.scalar_one_or_none()

            if not user:
                result["message"] = f"Usuário não encontrado no banco para user_id {user_id}"
                return result

            # Verificar se usuário está banido
            if user.is_banned:
                result["message"] = f"Usuário {user_id} está banido"
                return result

            # Obter cliente
            client = await get_user_client(user_id, session)
            if not client:
                result["message"] = f"Não foi possível criar cliente para user_id {user_id}"
                return result

            # Carregar grupos se necessário
            if user_id not in shared.user_group_list or not shared.get_user_groups(user_id):
                logger.info(f"Carregando grupos para usuário {user_id}")
                groups, admin_group_ids = await preload_groups_for_user(user_id, client)
            else:
                admin_group_ids = await get_admin_group_ids(user_id)
                groups = shared.get_user_groups(user_id)

            if not admin_group_ids:
                result["message"] = f"Nenhum grupo admin encontrado para user_id {user_id}"
                return result

            # Parse do link
            try:
                parts = link.split("/")
                if len(parts) < 2:
                    result["message"] = "Formato de link inválido"
                    return result
                peer = parts[-2]
                msg_id = int(parts[-1])
            except (ValueError, IndexError):
                result["message"] = "Link de mensagem inválido"
                return result

            # Obter entidade com retry
            entity = None
            for attempt_target in [peer, link, int(peer) if peer.isdigit() else None]:
                if attempt_target is None:
                    continue
                try:
                    entity = await asyncio.wait_for(
                        client.get_entity(attempt_target),
                        timeout=15
                    )
                    break
                except Exception as e:
                    logger.debug(f"Falha ao obter entidade para {attempt_target}: {e}")
                    continue

            if not entity:
                result["message"] = f"Não foi possível encontrar a origem da mensagem: {link}"
                return result

            # Obter mensagem
            try:
                message = await asyncio.wait_for(
                    client.get_messages(entity, ids=msg_id),
                    timeout=15
                )
                if not message:
                    result["message"] = f"Mensagem {msg_id} não encontrada"
                    return result
            except Exception as e:
                result["message"] = f"Erro ao obter mensagem: {str(e)}"
                return result

            # Filtrar apenas grupos onde o usuário é admin
            admin_groups = [g for g in groups if hasattr(g, 'id') and g.id in admin_group_ids]

            if not admin_groups:
                result["message"] = f"Nenhum grupo válido para encaminhamento"
                return result

            # Encaminhar mensagens
            successful, failed = await forward_message_batch(client, admin_groups, message, user_id)

            # Atualizar estatísticas globais
            shared.update_statistics('messages_sent', successful)
            shared.update_statistics('successful_forwards', successful)
            shared.update_statistics('failed_forwards', failed)

            if successful > 0:
                result["status"] = "success"
                result["message"] = f"Encaminhamento concluído: {successful} sucesso, {failed} falhas"
                result["successful"] = successful
                result["failed"] = failed
            else:
                result["message"] = f"Nenhuma mensagem foi encaminhada com sucesso. {failed} falhas."

            # Calcular tempo de execução
            execution_time = time.time() - start_time
            result["execution_time"] = execution_time

            logger.info(
                f"Encaminhamento para usuário {user_id}: "
                f"{successful} sucessos, {failed} falhas em {execution_time:.2f}s"
            )
            return result

    except asyncio.TimeoutError:
        result["message"] = f"Timeout durante operação para user_id {user_id}"
        logger.error(f"Timeout na operação para usuário {user_id}")
    except FloodWaitError as e:
        result["message"] = f"Rate limit atingido: aguarde {e.seconds} segundos"
        logger.warning(f"FloodWait para usuário {user_id}: {e.seconds}s")
    except PhoneNumberBannedError:
        result["message"] = f"Número de telefone banido para user_id {user_id}"
        logger.error(f"Usuário {user_id} com número banido")
    except Exception as e:
        result["message"] = f"Erro inesperado: {str(e)}"
        logger.error(f"Erro não tratado para usuário {user_id}: {str(e)}")
        # Log detalhado para debug
        import traceback
        logger.debug(f"Traceback completo: {traceback.format_exc()}")
    finally:
        # Limpar recursos se necessário
        execution_time = time.time() - start_time
        if execution_time > 60:  # Se demorou mais que 1 minuto
            logger.warning(f"Operação lenta para usuário {user_id}: {execution_time:.2f}s")
        return result

async def cleanup_expired_clients():
    """Limpa clientes expirados do cache"""
    async with cache_lock:
        current_time = time.time()
        expired_users = []
        for user_id, client_data in client_cache.items():
            if current_time - client_data['timestamp'] > CLIENT_CACHE_TTL:
                expired_users.append(user_id)
        for user_id in expired_users:
            try:
                client = client_cache[user_id]['client']
                await client.disconnect()
                logger.debug(f"Cliente desconectado e removido do cache: usuário {user_id}")
            except Exception as e:
                logger.warning(f"Erro ao desconectar cliente do usuário {user_id}: {e}")
            finally:
                client_cache.pop(user_id, None)
        if expired_users:
            logger.info(f"Removidos {len(expired_users)} clientes expirados do cache")

# Task para limpeza periódica
async def periodic_cleanup():
    """Executa limpeza periódica de clientes expirados"""
    while True:
        try:
            await cleanup_expired_clients()
            await asyncio.sleep(1800)  # A cada 30 minutos
        except Exception as e:
            logger.error(f"Erro na limpeza periódica: {e}")
            await asyncio.sleep(300)  # Retry em 5 minutos

# Iniciar task de limpeza
asyncio.create_task(periodic_cleanup())

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(json.dumps({"status": "error", "message": "Argumentos insuficientes"}))
        sys.exit(1)

    user_id = int(sys.argv[1])
    link = sys.argv[2]

    async def main():
        # Iniciar task de limpeza
        asyncio.create_task(periodic_cleanup())
        return await forward_message(user_id, link)

    try:
        result = asyncio.run(main())
        print(json.dumps(result))
    except Exception as e:
        error_result = {
            "status": "error",
            "message": f"Erro crítico: {str(e)}"
        }
        print(json.dumps(error_result))
        sys.exit(1)
