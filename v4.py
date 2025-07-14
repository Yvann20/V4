import os
import asyncio
import nest_asyncio
import re
import time
import requests
import base64
import logging
import msgpack
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
)
from telegram.ext import (
    ApplicationBuilder, ContextTypes, CommandHandler, CallbackQueryHandler,
    ConversationHandler, MessageHandler, filters
)
from telethon import TelegramClient
from telethon.errors import (
    SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberBannedError,
    MessageIdInvalidError, ChatAdminRequiredError, FloodWaitError
)
from telethon.tl.types import ChannelParticipantsAdmins

# Métricas Prometheus com registry dedicado
from prometheus_client import start_http_server, Counter, Gauge, Summary, CollectorRegistry, Histogram

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, BigInteger, String, DateTime, Boolean, UniqueConstraint, Date, select, func, Index
import redis
from rq import Queue
import mercadopago
import sqlite3
from tasks import forward_message_com_RQ
import shared

# Configurar asyncio e logging
nest_asyncio.apply()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()

# Configurações principais
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite+aiosqlite:///./meubanco.db")
MENU_IMAGE_URL = os.environ.get("MENU_IMAGE_URL", "https://i.imgur.com/8Q2Q5Qp.png")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
SUPORTE_LINK = os.environ.get("SUPORTE_LINK", "https://t.me/SEU_SUPORTE")
ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "@SEU_ADMIN")
MP_ACCESS_TOKEN = os.environ.get("MP_ACCESS_TOKEN")

# Configurações Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_MAX_CONNECTIONS = int(os.environ.get("REDIS_MAX_CONNECTIONS", 50))

# Configurações de performance
MAX_CONCURRENT_OPERATIONS = int(os.environ.get("MAX_CONCURRENT_OPERATIONS", 10))
SESSION_CHECK_INTERVAL = int(os.environ.get("SESSION_CHECK_INTERVAL", 300))
FLOOD_LIMIT = int(os.environ.get("FLOOD_LIMIT", 5))
FLOOD_INTERVAL = int(os.environ.get("FLOOD_INTERVAL", 10))
CLIENT_TIMEOUT = int(os.environ.get("CLIENT_TIMEOUT", 30))

# Configurações avançadas
ENABLE_CACHE_CLEANUP = os.environ.get("ENABLE_CACHE_CLEANUP", "true").lower() == "true"
CACHE_CLEANUP_INTERVAL = int(os.environ.get("CACHE_CLEANUP_INTERVAL", 1800))
ENABLE_SESSION_VALIDATION = os.environ.get("ENABLE_SESSION_VALIDATION", "true").lower() == "true"
ENABLE_AUTO_RETRY = os.environ.get("ENABLE_AUTO_RETRY", "true").lower() == "true"
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 3))

# Validar configurações essenciais
if not all([BOT_TOKEN, MENU_IMAGE_URL, OWNER_ID, MP_ACCESS_TOKEN]):
    raise ValueError('Configure BOT_TOKEN, MENU_IMAGE_URL, OWNER_ID e MP_ACCESS_TOKEN no .env')

# Configurar diretórios
SESSIONS_DIR = Path('sessions')
SESSIONS_DIR.mkdir(exist_ok=True, parents=True)

# Configurar SQLAlchemy
Base = declarative_base()

# Pool de conexões Redis otimizado
redis_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    max_connections=REDIS_MAX_CONNECTIONS,
    retry_on_timeout=True,
    socket_keepalive=True,
    socket_keepalive_options={}
)

redis_client = redis.StrictRedis(connection_pool=redis_pool, decode_responses=False)
rq_queue = Queue("bot_tasks", connection=redis_client)

# Engine do banco com pool otimizado
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
        "pool_size": 5,
        "max_overflow": 10,
        "pool_timeout": 20,
    })

engine = create_async_engine(DATABASE_URL, **engine_config)
AsyncSessionLocal = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# Configurar shared
shared.AsyncSessionLocal = AsyncSessionLocal
shared.rq_queue = rq_queue

# Configurar MercadoPago
mp = mercadopago.SDK(MP_ACCESS_TOKEN)
pending_payments = {}

# Cache e utilidades
def get_redis_con():
    """Retorna conexão Redis com fallback"""
    try:
        return redis.StrictRedis(connection_pool=redis_pool, decode_responses=False)
    except Exception as e:
        logger.error(f"Erro ao conectar Redis: {e}")
        return redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=False)

# Inicializar redis_client globalmente
redis_client = get_redis_con()

def cache_set(key, value, ttl=60*60*24*7):
    """Define valor no cache com TTL"""
    global redis_client
    try:
        packed = msgpack.packb(value, use_bin_type=True)
        redis_client.setex(key, ttl, packed)
    except redis.ConnectionError:
        redis_client = get_redis_con()
        redis_client.setex(key, ttl, packed)
    except Exception as e:
        logger.error(f"Erro ao salvar cache {key}: {e}")

def cache_get(key):
    """Obtém valor do cache"""
    global redis_client
    try:
        packed = redis_client.get(key)
        if packed is not None:
            return msgpack.unpackb(packed, raw=False)
        return None
    except redis.ConnectionError:
        redis_client = get_redis_con()
        packed = redis_client.get(key)
        if packed is not None:
            return msgpack.unpackb(packed, raw=False)
        return None
    except Exception as e:
        logger.error(f"Erro ao obter cache {key}: {e}")
        return None

def cache_delete(key):
    """Remove valor do cache"""
    global redis_client
    try:
        redis_client.delete(key)
    except redis.ConnectionError:
        redis_client = get_redis_con()
        redis_client.delete(key)
    except Exception as e:
        logger.error(f"Erro ao deletar cache {key}: {e}")

def load_cache(user_id):
    """Carrega cache do usuário"""
    key = f"usercache:{user_id}"
    val = cache_get(key)
    return val if val else {}

def save_cache(user_id, data):
    """Salva cache do usuário"""
    key = f"usercache:{user_id}"
    cache_set(key, data)

def update_cache(user_id, key_field, value):
    """Atualiza campo específico no cache"""
    data = load_cache(user_id)
    data[key_field] = value
    save_cache(user_id, data)

def clear_cache(user_id):
    """Limpa cache do usuário"""
    key = f"usercache:{user_id}"
    cache_delete(key)

# Limpeza periódica de cache
async def cache_cleanup_job():
    """Job para limpeza periódica de cache"""
    if not ENABLE_CACHE_CLEANUP:
        logger.info("Limpeza de cache desativada.")
        return
    while True:
        try:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
            logger.info("Iniciando limpeza de cache...")
            keys = redis_client.keys("usercache:*")
            deleted = 0
            for key in keys:
                try:
                    redis_client.delete(key)
                    deleted += 1
                except Exception as e:
                    logger.warning(f"Erro ao deletar cache {key}: {e}")
            logger.info(f"Limpeza de cache concluída: {deleted} itens removidos.")
        except Exception as e:
            logger.error(f"Erro na limpeza de cache: {e}")

# Modelos do banco de dados com índices otimizados
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

    __table_args__ = (
        UniqueConstraint('telegram_user_id', 'group_id', name='_user_group_uc'),
        Index('idx_user_groups', 'telegram_user_id'),
    )

class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id = Column(BigInteger, nullable=False, index=True)
    plan = Column(String, nullable=False)
    amount = Column(Integer, nullable=False)
    payment_id = Column(String, nullable=True, index=True)
    status = Column(String, nullable=False, default="pending", index=True)
    expires_at = Column(Date, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    message_id = Column(BigInteger, nullable=True)

    __table_args__ = (
        Index('idx_payment_user_status', 'telegram_user_id', 'status'),
        Index('idx_payment_expiry', 'expires_at', 'status'),
    )

shared.User = User

# Configuração de planos
PLANS = {
    "10d": {"label": "10 dias", "amount": 2, "days": 10},
    "1m": {"label": "1 mês", "amount": 20, "days": 30},
    "2m": {"label": "2 meses", "amount": 40, "days": 60},
}

# Registry Prometheus dedicado
PROM_REGISTRY = CollectorRegistry(auto_describe=True)
messages_sent_counter = Counter('bot_messages_sent_total', 'Total de mensagens enviadas', registry=PROM_REGISTRY)
active_campaigns_gauge = Gauge('bot_active_campaigns', 'Total de campanhas ativas', registry=PROM_REGISTRY)
message_forward_time = Summary('bot_message_forward_duration_seconds', 'Tempo para encaminhar mensagens', registry=PROM_REGISTRY)
user_operations = Histogram('bot_user_operations_duration_seconds', 'Duração das operações por usuário', ['operation'], registry=PROM_REGISTRY)
error_counter = Counter('bot_errors_total', 'Total de erros', ['error_type'], registry=PROM_REGISTRY)

shared.messages_sent_counter = messages_sent_counter
shared.active_campaigns_gauge = active_campaigns_gauge
shared.message_forward_time = message_forward_time

# Sistema anti-flood otimizado
user_flood = {}

def antiflood_check(user_id):
    """Verifica rate limiting com cleanup automático"""
    now = time.time()
    if user_id not in user_flood:
        user_flood[user_id] = []

    # Limpar requests antigos
    user_flood[user_id] = [t for t in user_flood[user_id] if now - t < FLOOD_INTERVAL]

    if len(user_flood[user_id]) >= FLOOD_LIMIT:
        return False

    user_flood[user_id].append(now)
    return True

def antiflood(func):
    """Decorator para prevenção de flood"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id

        if not antiflood_check(user_id):
            try:
                if update.message:
                    await update.message.reply_text("⏳ Muitas tentativas. Aguarde um pouco antes de tentar novamente.")
                elif update.callback_query:
                    await update.callback_query.answer("⏳ Muitas tentativas. Aguarde um pouco.", show_alert=True)
            except Exception:
                pass
            return

        return await func(update, context, *args, **kwargs)
    return wrapper

# Estados do ConversationHandler
(
    AUTH_PHONE, AUTH_API_ID, AUTH_API_HASH, AUTH_CODE, AUTH_PASSWORD,
    LINK, INTERVAL, CONFIRM_SESSION_REPLACE
) = range(8)

# Controle de conversações de login
active_login_conversations = set()

async def init_db():
    """Inicializa o banco de dados"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Banco de dados inicializado")

def get_session_path(telegram_user_id, phone):
    """Gera caminho para arquivo de sessão"""
    user_dir = SESSIONS_DIR / str(telegram_user_id)
    user_dir.mkdir(exist_ok=True, parents=True)
    return str(user_dir / f"{phone.replace('+','')}_{telegram_user_id}.session")

def has_active_campaign(user_id):
    """Verifica se usuário tem campanha ativa"""
    return (
        user_id in shared.active_campaigns and
        shared.active_campaigns[user_id].get('job_id') is not None
    )

def log_error(msg, exc=None):
    """Log de erros com contexto"""
    logger.error(msg)
    if exc:
        logger.exception("Detalhes do erro:")
        error_counter.labels(error_type=type(exc).__name__).inc()

def parse_code(text):
    """Parse do código de verificação para aceitar formatos como 1,2,3,4,5 ou bot1,2,3,4,5"""
    text = text.strip().lower()
    if text.startswith('bot'):
        text = text[3:]
    digits = [d.strip() for d in text.split(',') if d.strip().isdigit()]
    return ''.join(digits)

def is_valid_message_link(link):
    """Valida link de mensagem"""
    pattern = r'^(https?://)?t\.me/[^/]+/\d+$'
    return re.match(pattern, link) is not None

def is_valid_interval(interval):
    """Valida intervalo"""
    return interval.isdigit() and 1 <= int(interval) <= 1440  # Max 24 horas

@user_operations.labels(operation='check_payment').time()
async def user_has_active_payment(user_id):
    """Verifica se usuário tem pagamento ativo com cache"""
    cache_key = f"payment_status:{user_id}"
    cached = cache_get(cache_key)

    if cached is not None:
        return cached.get('has_active', False)

    async with shared.AsyncSessionLocal() as session:
        stmt = select(Payment).where(
            Payment.telegram_user_id == user_id,
            Payment.status == "approved",
            Payment.expires_at >= datetime.now().date()
        )
        result = await session.execute(stmt)
        payment = result.scalar_one_or_none()

        has_active = payment is not None

        # Cache por 30 minutos
        cache_set(cache_key, {'has_active': has_active}, ttl=1800)

        return has_active

async def user_payment_expiry(user_id):
    """Obtém data de expiração do pagamento"""
    async with shared.AsyncSessionLocal() as session:
        stmt = select(Payment).where(
            Payment.telegram_user_id == user_id,
            Payment.status == "approved",
            Payment.expires_at >= datetime.now().date()
        ).order_by(Payment.expires_at.desc())

        result = await session.execute(stmt)
        payment = result.scalar_one_or_none()

        if payment:
            return payment.expires_at
        return None

@user_operations.labels(operation='create_payment').time()
async def gerar_pagamento_pix(query, valor: float, context, plan_key):
    """Gera pagamento PIX com tratamento de erro melhorado"""
    usuario_id = query.from_user.id

    dados_pagamento = {
        "transaction_amount": valor,
        "description": f"Plano {PLANS[plan_key]['label']} - BOT TG",
        "payment_method_id": "pix",
        "payer": {"email": f"user{usuario_id}@emailfake.com"},
        "notification_url": f"https://seusite.com/webhook/payment/{usuario_id}"  # Webhook opcional
    }

    try:
        logger.info(f"Gerando pagamento PIX para usuário {usuario_id}, plano {plan_key}, valor R${valor:.2f}")
        resposta = mp.payment().create(dados_pagamento)
        pagamento = resposta.get("response", {})

        if pagamento.get("status") == "pending":
            codigo_pix = pagamento["point_of_interaction"]["transaction_data"]["qr_code"]
            qr_code_base64 = pagamento["point_of_interaction"]["transaction_data"]["qr_code_base64"]
            id_pagamento = str(pagamento["id"])

            pending_payments[usuario_id] = id_pagamento

            # Salvar no banco
            async with shared.AsyncSessionLocal() as session:
                # Remove pagamentos pendentes antigos
                stmt = select(Payment).where(
                    Payment.telegram_user_id == usuario_id,
                    Payment.status == "pending"
                )
                result = await session.execute(stmt)
                old_payments = result.scalars().all()

                for old in old_payments:
                    await session.delete(old)

                payment = Payment(
                    telegram_user_id=usuario_id,
                    plan=plan_key,
                    amount=valor,
                    payment_id=id_pagamento,
                    status="pending"
                )
                session.add(payment)
                await session.commit()

            # Preparar imagem QR
            img_bytes = BytesIO(base64.b64decode(qr_code_base64))
            img_bytes.seek(0)

            caption = (
                f"✅ <b>Pagamento PIX Gerado!</b>\n\n"
                f"💳 <b>Plano:</b> <code>{PLANS[plan_key]['label']}</code>\n"
                f"💰 <b>Valor:</b> <code>R$ {valor:.2f}</code>\n\n"
                f"📷 <b>Escaneie o QR Code abaixo para pagar:</b>\n\n"
                f"📎 <b>Copia e Cola:</b>\n"
                f"<code>{codigo_pix}</code>\n\n"
                f"⏳ <i>Assim que o pagamento for aprovado, seu acesso será liberado automaticamente.</i>"
            )

            mensagem_enviada = await context.bot.send_photo(
                chat_id=usuario_id,
                photo=img_bytes,
                caption=caption,
                parse_mode="HTML"
            )

            id_mensagem = mensagem_enviada.message_id

            # Atualizar com message_id
            async with shared.AsyncSessionLocal() as session:
                stmt = select(Payment).where(Payment.payment_id == id_pagamento)
                result = await session.execute(stmt)
                payment = result.scalar_one_or_none()
                if payment:
                    payment.message_id = id_mensagem
                    await session.commit()

            # Iniciar verificação
            asyncio.create_task(
                verificar_status_pagamento(usuario_id, id_pagamento, id_mensagem, context, valor, plan_key)
            )
            logger.info(f"Pagamento PIX gerado com sucesso para usuário {usuario_id}, ID {id_pagamento}")

        else:
            logger.warning(f"Status de pagamento inesperado para usuário {usuario_id}: {pagamento.get('status')}")
            await query.message.reply_text("❌ Erro ao gerar o pagamento. Tente novamente.")

    except Exception as e:
        logger.error(f"Erro ao criar pagamento para usuário {usuario_id}: {e}", exc_info=True)
        error_counter.labels(error_type='payment_creation').inc()
        await query.message.reply_text("❌ Ocorreu um erro ao gerar o pagamento. Tente novamente mais tarde.")

async def verificar_status_pagamento(usuario_id, id_pagamento, id_mensagem, context, valor, plan_key):
    """Verifica status do pagamento com retry otimizado"""
    tentativas = 0
    max_tentativas = 30
    intervalo_verificacao = 20

    while tentativas < max_tentativas:
        await asyncio.sleep(intervalo_verificacao)
        tentativas += 1

        try:
            logger.info(f"Verificando status do pagamento {id_pagamento} para usuário {usuario_id}, tentativa {tentativas}/{max_tentativas}")
            info_pagamento = mp.payment().get(id_pagamento)
            status = info_pagamento.get("response", {}).get("status", "")

            if status == "approved":
                logger.info(f"Pagamento {id_pagamento} aprovado para usuário {usuario_id}")
                # Remover mensagem do PIX
                try:
                    await context.bot.delete_message(chat_id=usuario_id, message_id=id_mensagem)
                except Exception as e:
                    logger.warning(f"Erro ao deletar mensagem PIX para {usuario_id}: {e}")

                # Atualizar banco
                async with shared.AsyncSessionLocal() as session:
                    stmt = select(Payment).where(
                        Payment.payment_id == id_pagamento,
                        Payment.telegram_user_id == usuario_id
                    )
                    result = await session.execute(stmt)
                    payment = result.scalar_one_or_none()

                    if payment:
                        plan_days = PLANS[plan_key]["days"]
                        expires_at = datetime.now().date() + timedelta(days=plan_days)
                        payment.status = "approved"
                        payment.expires_at = expires_at
                        await session.commit()

                        # Desbanir usuário se estiver banido
                        stmt2 = select(shared.User).where(shared.User.telegram_user_id == usuario_id)
                        result2 = await session.execute(stmt2)
                        user = result2.scalar_one_or_none()
                        if user:
                            user.is_banned = False
                            await session.commit()

                # Limpar cache de pagamento
                cache_delete(f"payment_status:{usuario_id}")

                # Notificar usuário
                await context.bot.send_message(
                    chat_id=usuario_id,
                    text=f"✅ <b>Pagamento Confirmado!</b>\n\nSeu acesso está liberado até {expires_at.strftime('%d/%m/%Y')}.",
                    parse_mode="HTML"
                )

                # Notificar admin (opcional)
                try:
                    usuario_info = await context.bot.get_chat(usuario_id)
                    usuario_nome = usuario_info.first_name or "Desconhecido"
                    await context.bot.send_message(
                        chat_id=OWNER_ID,
                        text=(
                            f"🟢 <b>Novo pagamento aprovado</b>\n\n"
                            f"👤 <b>ID:</b> <code>{usuario_id}</code>\n"
                            f"👥 <b>Nome:</b> <code>{usuario_nome}</code>\n"
                            f"💰 <b>Valor:</b> <code>R$ {valor:.2f}</code>\n"
                            f"📦 <b>Plano:</b> <code>{PLANS[plan_key]['label']}</code>"
                        ),
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Erro ao notificar admin sobre pagamento de {usuario_id}: {e}")

                # Remover dos pagamentos pendentes
                if usuario_id in pending_payments:
                    del pending_payments[usuario_id]

                return

            elif status in ["cancelled", "rejected"]:
                logger.info(f"Pagamento {id_pagamento} cancelado/rejeitado para usuário {usuario_id}")
                # Pagamento rejeitado/cancelado
                try:
                    await context.bot.edit_message_caption(
                        chat_id=usuario_id,
                        message_id=id_mensagem,
                        caption="❌ <b>Pagamento cancelado/rejeitado</b>\n\nTente novamente se necessário.",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    logger.warning(f"Erro ao editar mensagem de pagamento cancelado para {usuario_id}: {e}")

                if usuario_id in pending_payments:
                    del pending_payments[usuario_id]
                return

        except Exception as e:
            logger.error(f"Erro ao verificar status do pagamento {id_pagamento} para {usuario_id}: {e}")
            error_counter.labels(error_type='payment_verification').inc()

    # Timeout - pagamento não confirmado
    logger.info(f"Tempo esgotado para pagamento {id_pagamento} do usuário {usuario_id}")
    try:
        await context.bot.edit_message_caption(
            chat_id=usuario_id,
            message_id=id_mensagem,
            caption="⏰ <b>Tempo para pagamento expirado</b>\n\nGere um novo PIX se ainda desejar adquirir o plano.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"Erro ao editar mensagem de pagamento expirado para {usuario_id}: {e}")

    if usuario_id in pending_payments:
        del pending_payments[usuario_id]

async def session_validation_job(app):
    """Job para validação periódica de sessões"""
    if not ENABLE_SESSION_VALIDATION:
        logger.info("Validação de sessões desativada.")
        return
    while True:
        try:
            await asyncio.sleep(SESSION_CHECK_INTERVAL)
            logger.info("Iniciando validação periódica de sessões...")

            async with shared.AsyncSessionLocal() as session:
                stmt = select(shared.User).where(shared.User.is_authenticated == True)
                result = await session.execute(stmt)
                users = result.scalars().all()

                disconnected_users = []

                for user in users:
                    try:
                        client = shared.user_clients.get(user.telegram_user_id)

                        if client:
                            if not client.is_connected() or not await client.is_user_authorized():
                                await client.disconnect()
                                user.is_authenticated = False
                                shared.user_clients.pop(user.telegram_user_id, None)
                                disconnected_users.append(user.telegram_user_id)
                        else:
                            # Cliente não existe, verificar se arquivo de sessão existe
                            if user.session_path and not os.path.exists(user.session_path):
                                user.is_authenticated = False
                                disconnected_users.append(user.telegram_user_id)

                    except Exception as e:
                        logger.warning(f"Erro ao validar sessão do usuário {user.telegram_user_id}: {e}")
                        user.is_authenticated = False
                        shared.user_clients.pop(user.telegram_user_id, None)
                        disconnected_users.append(user.telegram_user_id)

                if disconnected_users:
                    await session.commit()
                    logger.info(f"Desconectados {len(disconnected_users)} usuários com sessões inválidas")

                # Notificar usuários desconectados
                for user_id in disconnected_users:
                    try:
                        await app.bot.send_message(
                            user_id,
                            "⚠️ <b>Sua sessão expirou</b>\n\nÉ necessário fazer login novamente para usar o bot.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Erro ao notificar usuário {user_id} sobre sessão expirada: {e}")

        except Exception as e:
            logger.error(f"Erro no job de validação de sessões: {e}")
            error_counter.labels(error_type='session_validation').inc()

@user_operations.labels(operation='send_menu').time()
async def send_menu(user_id, context):
    """Envia menu principal otimizado"""
    try:
        # Buscar dados em paralelo
        payment_task = asyncio.create_task(user_has_active_payment(user_id))
        expiry_task = asyncio.create_task(user_payment_expiry(user_id))

        async with shared.AsyncSessionLocal() as session:
            stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()

        pago = await payment_task
        validade = await expiry_task

        autenticado = "✅ Sim" if user and user.is_authenticated else "❌ Não"
        campanha = "✅ Ativa" if has_active_campaign(user_id) else "❌ Nenhuma"

        if validade:
            validade_str = validade.strftime('%d/%m/%Y')
        else:
            validade_str = "❌ Nenhum"

        plano_status = "✅ Ativo" if pago else "❌ Inativo"

        # Estatísticas do usuário
        user_stats = shared.statistics.get(f'user_{user_id}', {})
        messages_count = user_stats.get('messages_sent', 0)

        menu_text = (
            f"👋 <b>Bem-vindo ao Painel de Campanhas</b>\n\n"
            f"📊 <b>Status da sua conta:</b>\n"
            f"• 🔑 Autenticado: {autenticado}\n"
            f"• 🚀 Campanha ativa: {campanha}\n"
            f"• 🗓️ Plano: {plano_status}\n"
            f"• 📅 Expira em: {validade_str}\n"
            f"• 📨 Mensagens enviadas: {messages_count}\n\n"
            f"👇 <b>Escolha uma opção:</b>"
        )

        keyboard = [
            [InlineKeyboardButton("📦 Planos", callback_data='payment_menu')],
            [
                InlineKeyboardButton("🔐 Login", callback_data='login'),
                InlineKeyboardButton("🚀 Nova Campanha", callback_data='create_campaign')
            ],
            [
                InlineKeyboardButton("🛑 Cancelar Campanha", callback_data='cancel_campaign'),
                InlineKeyboardButton("📊 Estatísticas", callback_data='statistics')
            ],
            [
                InlineKeyboardButton("🔄 Atualizar Grupos", callback_data='update_groups'),
                InlineKeyboardButton("❓ Suporte", url=SUPORTE_LINK)
            ]
        ]

        try:
            await context.bot.send_photo(
                chat_id=user_id,
                photo=MENU_IMAGE_URL,
                caption=menu_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception as e:
            # Fallback para texto se imagem falhar
            logger.warning(f"Falha ao enviar imagem do menu para {user_id}: {e}")
            await context.bot.send_message(
                chat_id=user_id,
                text=menu_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

    except Exception as e:
        logger.error(f"Erro ao enviar menu para {user_id}: {e}", exc_info=True)
        error_counter.labels(error_type='send_menu').inc()

        try:
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Erro ao carregar menu. Tente novamente em alguns instantes."
            )
        except Exception:
            pass

# Handlers principais

@antiflood
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler do comando /start"""
    user = update.effective_user
    user_id = user.id
    nome = user.full_name or "usuário"

    # Registrar usuário se não existir
    async with shared.AsyncSessionLocal() as session:
        stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
        result = await session.execute(stmt)
        db_user = result.scalar_one_or_none()

        if not db_user:
            db_user = shared.User(telegram_user_id=user_id)
            session.add(db_user)
            await session.commit()
            logger.info(f"Novo usuário registrado: {user_id}")

    mensagem = (
        f"👋 Olá, <b>{nome}</b>!\n\n"
        f"🧾 <b>Seu ID:</b> <code>{user_id}</code>\n\n"
        f"Bem-vindo ao nosso bot profissional de campanhas Telegram.\n\n"
        f"Use o menu abaixo para começar:"
    )

    await update.message.reply_text(mensagem, parse_mode="HTML")
    await send_menu(user_id, context)

@antiflood
async def payment_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Menu de pagamentos"""
    user_id = update.effective_user.id

    # Verificar se já tem plano ativo
    has_active = await user_has_active_payment(user_id)
    expiry = await user_payment_expiry(user_id)

    if has_active and expiry:
        status_text = f"✅ <b>Você já possui um plano ativo!</b>\n\nExpira em: {expiry.strftime('%d/%m/%Y')}\n\n"
    else:
        status_text = "💳 <b>Escolha um plano para liberar o acesso:</b>\n\n"

    plan_text = ""
    keyboard = []

    for plan_key, plan_info in PLANS.items():
        plan_text += f"📦 <b>{plan_info['label']}</b> - R$ {plan_info['amount']}\n"
        keyboard.append([
            InlineKeyboardButton(
                f"💳 {plan_info['label']} - R$ {plan_info['amount']}",
                callback_data=f"pay_{plan_key}"
            )
        ])

    keyboard.append([InlineKeyboardButton("⬅️ Voltar ao menu", callback_data="main_menu")])

    final_text = status_text + plan_text

    try:
        if update.callback_query:
            # Tentar editar a mensagem, mas usar um fallback se falhar
            try:
                await update.callback_query.edit_message_text(
                    final_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            except Exception as e:
                logger.warning(f"Erro ao editar mensagem para {user_id}: {e}. Enviando nova mensagem.")
                # Se não for possível editar, enviar uma nova mensagem
                await context.bot.send_message(
                    user_id,
                    final_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
                # Tentar deletar a mensagem anterior, se possível
                try:
                    await update.callback_query.message.delete()
                except Exception:
                    pass
        else:
            await context.bot.send_message(
                user_id,
                final_text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"Erro no menu de pagamento para {user_id}: {e}", exc_info=True)
        await context.bot.send_message(
            user_id,
            "❌ Erro ao carregar menu de pagamentos. Tente novamente.",
            parse_mode="HTML"
        )



@antiflood
async def payment_button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler dos botões de pagamento"""
    query = update.callback_query
    user_id = update.effective_user.id
    plan_key = query.data.replace("pay_", "")

    if plan_key not in PLANS:
        await query.answer("❌ Plano inválido!", show_alert=True)
        logger.warning(f"Plano inválido selecionado por {user_id}: {plan_key}")
        return

    # Verificar se já tem plano ativo
    if await user_has_active_payment(user_id):
        await query.answer("⚠️ Você já possui um plano ativo!", show_alert=True)
        logger.info(f"Usuário {user_id} tentou comprar plano com um ativo existente")
        return

    plan = PLANS[plan_key]
    logger.info(f"Usuário {user_id} selecionou plano {plan_key} com valor R${plan['amount']}")
    await gerar_pagamento_pix(query, plan["amount"], context, plan_key)

@antiflood
async def cancel_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancela campanha ativa"""
    user_id = update.effective_user.id

    if not await user_has_active_payment(user_id):
        await context.bot.send_message(
            user_id,
            "❌ Você não possui um plano ativo. Compre um plano para liberar o uso do bot."
        )
        return ConversationHandler.END

    if not has_active_campaign(user_id):
        await context.bot.send_message(user_id, "⚠️ Nenhuma campanha ativa para cancelar!")
        return ConversationHandler.END

    try:
        job_id = shared.active_campaigns[user_id]['job_id']
        shared.rq_queue.remove(job_id)
        del shared.active_campaigns[user_id]

        shared.statistics['active_campaigns'] = len(shared.active_campaigns)
        shared.active_campaigns_gauge.set(len(shared.active_campaigns))

        await context.bot.send_message(user_id, "🛑 Campanha cancelada com sucesso.")
        logger.info(f"Campanha cancelada para usuário {user_id}")

    except Exception as e:
        logger.error(f"Erro ao cancelar campanha para {user_id}: {e}", exc_info=True)
        await context.bot.send_message(user_id, "❌ Erro ao cancelar campanha. Tente novamente.")

    return ConversationHandler.END

@antiflood
async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Mostra estatísticas do bot"""
    user_id = update.effective_user.id

    try:
        # Inicializar estatísticas se não existirem
        if 'statistics' not in shared.__dict__:
            shared.statistics = {
                'messages_sent': 0,
                'active_campaigns': 0
            }
            logger.warning("Estatísticas globais não inicializadas, criando estrutura padrão.")

        # Estatísticas globais
        global_stats = shared.statistics

        # Estatísticas do usuário
        user_stats = global_stats.get(f'user_{user_id}', {
            'messages_sent': 0,
            'successful_sends': 0,
            'failed_sends': 0
        })

        # Estatísticas da fila
        queue_length = len(shared.rq_queue) if hasattr(shared.rq_queue, '__len__') else 0
        failed_jobs = 0
        try:
            failed_jobs = len(shared.rq_queue.failed_job_registry) if hasattr(shared.rq_queue, 'failed_job_registry') else 0
        except Exception as e:
            logger.warning(f"Erro ao acessar failed_job_registry: {e}")

        stats_text = (
            f"📊 <b>Estatísticas do Sistema</b>\n\n"
            f"🌐 <b>Globais:</b>\n"
            f"• 📨 Mensagens enviadas: {global_stats.get('messages_sent', 0)}\n"
            f"• 🚀 Campanhas ativas: {global_stats.get('active_campaigns', 0)}\n"
            f"• ⏳ Tarefas na fila: {queue_length}\n"
            f"• ❌ Tarefas falhadas: {failed_jobs}\n\n"
            f"👤 <b>Suas estatísticas:</b>\n"
            f"• 📨 Mensagens enviadas: {user_stats.get('messages_sent', 0)}\n"
            f"• ✅ Envios bem-sucedidos: {user_stats.get('successful_sends', 0)}\n"
            f"• ❌ Envios falhados: {user_stats.get('failed_sends', 0)}\n"
        )

        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    stats_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Voltar ao menu", callback_data="main_menu")]
                    ])
                )
            except Exception as e:
                logger.warning(f"Erro ao editar mensagem de estatísticas para {user_id}: {e}. Enviando nova mensagem.")
                await context.bot.send_message(
                    user_id,
                    stats_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("⬅️ Voltar ao menu", callback_data="main_menu")]
                    ])
                )
                # Tentar deletar a mensagem anterior, se possível
                try:
                    await update.callback_query.message.delete()
                except Exception:
                    pass
        else:
            await context.bot.send_message(
                user_id,
                stats_text,
                parse_mode="HTML"
            )

    except Exception as e:
        logger.error(f"Erro ao mostrar estatísticas para {user_id}: {e}", exc_info=True)
        await context.bot.send_message(user_id, "❌ Erro ao carregar estatísticas.")


@antiflood
async def start_campaign(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inicia nova campanha"""
    user_id = update.effective_user.id

    # Verificações básicas
    if not await user_has_active_payment(user_id):
        await context.bot.send_message(
            user_id,
            "❌ Você não possui um plano ativo. Compre um plano para liberar o uso do bot."
        )
        return ConversationHandler.END

    async with shared.AsyncSessionLocal() as session:
        stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if user and user.is_banned:
            await context.bot.send_message(
                user_id,
                "🚫 Você está banido do bot. Entre em contato com o suporte se achar que isso é um erro."
            )
            return ConversationHandler.END

        if not (user and user.is_authenticated):
            await context.bot.send_message(
                user_id,
                "🚫 Você precisa fazer login da sua conta Telethon primeiro! Use o menu para fazer login."
            )
            return ConversationHandler.END

    if has_active_campaign(user_id):
        await context.bot.send_message(
            user_id,
            "⚠️ Você já tem uma campanha ativa! Cancele a atual antes de iniciar uma nova."
        )
        return ConversationHandler.END

    # Inicializar configurações da campanha
    shared.user_settings[user_id] = {"message_link": None, "interval": None}

    await context.bot.send_message(
        user_id,
        '📝 Envie o link da mensagem a ser encaminhada:\n\n'
        '🔗 <b>Exemplo:</b> <code>https://t.me/seucanal/123</code>\n\n'
        '⚠️ <i>Certifique-se de que você tem acesso à mensagem.</i>',
        parse_mode="HTML"
    )
    return LINK

@antiflood
async def set_message_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Define link da mensagem"""
    user_id = update.effective_user.id
    link = update.message.text.strip()

    if not is_valid_message_link(link):
        await update.message.reply_text(
            "❌ O link informado é inválido.\n\n"
            "📋 <b>Formato correto:</b> <code>https://t.me/seucanal/123</code>\n\n"
            "💡 <i>Tente novamente com um link válido.</i>",
            parse_mode="HTML"
        )
        return LINK

    shared.user_settings[user_id]["message_link"] = link
    update_cache(user_id, "msg_link", link)

    await update.message.reply_text(
        "✅ <b>Link salvo com sucesso!</b>\n\n"
        "⏰ Agora envie o intervalo em <b>minutos</b> entre os envios:\n\n"
        "📋 <b>Exemplos:</b>\n"
        "• <code>5</code> - A cada 5 minutos\n"
        "• <code>15</code> - A cada 15 minutos\n"
        "• <code>60</code> - A cada 1 hora\n\n"
        "⚠️ <i>Mínimo: 1 minuto | Máximo: 1440 minutos (24h)</i>",
        parse_mode="HTML"
    )
    return INTERVAL

@antiflood
async def set_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Define intervalo da campanha"""
    user_id = update.effective_user.id
    interval = update.message.text.strip()

    if not is_valid_interval(interval):
        await update.message.reply_text(
            "⚠️ <b>Intervalo inválido!</b>\n\n"
            "📋 <b>Regras:</b>\n"
            "• Apenas números inteiros\n"
            "• Mínimo: 1 minuto\n"
            "• Máximo: 1440 minutos (24 horas)\n\n"
            "📝 <b>Exemplo:</b> <code>10</code>",
            parse_mode="HTML"
        )
        return INTERVAL

    try:
        interval = int(interval)

        # Validar acesso à conta Telethon
        async with shared.AsyncSessionLocal() as session:
            client = await get_user_client(user_id, session)
            if client is None:
                await update.message.reply_text(
                    "❌ <b>Erro ao acessar sua conta Telethon</b>\n\n"
                    "🔄 Faça login novamente usando o menu principal.",
                    parse_mode="HTML"
                )
                return ConversationHandler.END

            # Carregar grupos do usuário
            await update.message.reply_text("⏳ <b>Carregando seus grupos...</b>", parse_mode="HTML")
            await preload_groups_for_user(user_id, client, context)

        # Adicionar jitter para distribuir carga
        import random
        jitter = random.randint(0, min(30, interval * 60 // 10))  # Até 10% do intervalo ou 30s
        delay = timedelta(minutes=interval, seconds=jitter)

        # Enfileirar primeira tarefa com verificação
        try:
            job = shared.rq_queue.enqueue_in(delay, forward_message_com_RQ, user_id)
            if job is None:
                raise ValueError("Job não foi criado corretamente.")
        except Exception as e:
            logger.error(f"Erro ao enfileirar tarefa para usuário {user_id}: {e}", exc_info=True)
            await update.message.reply_text(
                "❌ <b>Erro ao iniciar campanha</b>\n\n"
                "🔄 Tente novamente ou entre em contato com o suporte.",
                parse_mode="HTML"
            )
            return ConversationHandler.END

        # Registrar campanha ativa
        shared.active_campaigns[user_id] = {
            'job_id': job.id,
            'start_time': asyncio.get_event_loop().time(),
            'interval': interval,
            'message_link': shared.user_settings[user_id]["message_link"]
        }

        shared.statistics['active_campaigns'] = len(shared.active_campaigns)
        shared.active_campaigns_gauge.set(len(shared.active_campaigns))

        # Salvar configurações no cache
        shared.user_settings[user_id]["interval"] = interval
        update_cache(user_id, "interval", interval)

        logger.info(f"Campanha iniciada para usuário {user_id} com intervalo de {interval} minutos, job_id {job.id}")

        await update.message.reply_text(
            f"🎉 <b>Campanha iniciada com sucesso!</b>\n\n"
            f"⏰ <b>Intervalo:</b> {interval} minutos\n"
            f"🔗 <b>Link:</b> <code>{shared.user_settings[user_id]['message_link']}</code>\n"
            f"📅 <b>Próximo envio:</b> {(datetime.now() + delay).strftime('%H:%M:%S')}\n\n"
            f"🛑 <i>Use o menu para cancelar a campanha quando necessário.</i>",
            parse_mode="HTML"
        )

    except Exception as e:
        logger.error(f"Erro ao iniciar campanha para {user_id}: {e}", exc_info=True)
        error_counter.labels(error_type='campaign_start').inc()
        await update.message.reply_text(
            "❌ <b>Ocorreu um erro ao iniciar sua campanha</b>\n\n"
            "🔄 Tente novamente ou fale com o suporte se o problema persistir.",
            parse_mode="HTML"
        )

    return ConversationHandler.END

async def get_user_client(telegram_user_id, session):
    """Obtém cliente Telethon do usuário com retry"""
    try:
        stmt = select(shared.User).where(
            shared.User.telegram_user_id == telegram_user_id,
            shared.User.is_authenticated == True
        )
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            logger.warning(f"Usuário não encontrado no banco para ID {telegram_user_id}")
            return None

        # Verificar se já existe cliente ativo
        client = shared.user_clients.get(telegram_user_id)
        if client:
            if client.is_connected() and await client.is_user_authorized():
                return client
            else:
                # Cliente inválido, remover
                await client.disconnect()
                del shared.user_clients[telegram_user_id]

        # Criar novo cliente com retry
        if ENABLE_AUTO_RETRY:
            for attempt in range(MAX_RETRIES):
                try:
                    client = TelegramClient(user.session_path, user.api_id, user.api_hash, timeout=CLIENT_TIMEOUT)
                    await client.connect()
                    if not await client.is_user_authorized():
                        await client.disconnect()
                        logger.warning(f"Cliente não autorizado para {telegram_user_id}")
                        return None
                    shared.user_clients[telegram_user_id] = client
                    logger.info(f"Cliente Telethon criado para {telegram_user_id} na tentativa {attempt+1}")
                    return client
                except Exception as e:
                    logger.warning(f"Tentativa {attempt+1} falhou para criar cliente de {telegram_user_id}: {e}")
                    if attempt == MAX_RETRIES - 1:
                        logger.error(f"Erro ao criar cliente para usuário {telegram_user_id} após {MAX_RETRIES} tentativas: {e}")
                        return None
                    await asyncio.sleep(2 ** attempt)  # Backoff exponencial
        else:
            try:
                client = TelegramClient(user.session_path, user.api_id, user.api_hash, timeout=CLIENT_TIMEOUT)
                await client.connect()
                if not await client.is_user_authorized():
                    await client.disconnect()
                    logger.warning(f"Cliente não autorizado para {telegram_user_id}")
                    return None
                shared.user_clients[telegram_user_id] = client
                logger.info(f"Cliente Telethon criado para {telegram_user_id}")
                return client
            except Exception as e:
                logger.error(f"Erro ao criar cliente para usuário {telegram_user_id}: {e}")
                return None
    except Exception as e:
        logger.error(f"Erro geral ao obter cliente para usuário {telegram_user_id}: {e}", exc_info=True)
        return None

async def preload_groups_for_user(telegram_user_id, client, context=None):
    """Carrega grupos do usuário"""
    try:
        if context:
            try:
                await context.bot.send_message(telegram_user_id, "🔍 <b>Analisando grupos...</b>", parse_mode="HTML")
            except Exception:
                pass

        groups = []
        admin_group_ids = []

        me = await client.get_me()

        # Usar semáforo para controlar requisições simultâneas
        semaphore = asyncio.Semaphore(5)

        async def check_group(dialog):
            async with semaphore:
                try:
                    if dialog.is_group and not dialog.archived:
                        groups.append(dialog.entity)
                        is_admin = await is_user_admin_in_group(client, dialog.entity, me.id)
                        if is_admin and hasattr(dialog.entity, "id"):
                            admin_group_ids.append(dialog.entity.id)
                except Exception as e:
                    logger.warning(f"Erro ao verificar grupo para usuário {telegram_user_id}: {e}")

        # Processar grupos em lotes
        tasks = []
        async for dialog in client.iter_dialogs():
            if dialog.is_group and not dialog.archived:
                tasks.append(check_group(dialog))

                # Processar em lotes de 10
                if len(tasks) >= 10:
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks = []

        # Processar tarefas restantes
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Armazenar resultados
        shared.user_group_list[telegram_user_id] = groups
        update_cache(telegram_user_id, "groups", [g.id for g in groups])

        # Salvar grupos admin no banco
        async with shared.AsyncSessionLocal() as session:
            # Remover grupos admin antigos
            await session.execute(
                UserAdminGroup.__table__.delete().where(
                    UserAdminGroup.telegram_user_id == telegram_user_id
                )
            )

            # Adicionar novos grupos admin
            for gid in admin_group_ids:
                session.add(UserAdminGroup(telegram_user_id=telegram_user_id, group_id=gid))

            await session.commit()

        if context:
            try:
                await context.bot.send_message(
                    telegram_user_id,
                    f"✅ <b>Análise concluída!</b>\n\n"
                    f"📊 <b>Resultados:</b>\n"
                    f"• 🏢 Total de grupos: {len(groups)}\n"
                    f"• 👑 Grupos onde você é admin: {len(admin_group_ids)}\n\n"
                    f"💡 <i>Mensagens serão enviadas apenas para os grupos onde você é administrador.</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        logger.info(f"Grupos carregados para usuário {telegram_user_id}: {len(groups)} total, {len(admin_group_ids)} admin")

        return groups

    except Exception as e:
        logger.error(f"Erro ao carregar grupos para usuário {telegram_user_id}: {e}", exc_info=True)
        if context:
            try:
                await context.bot.send_message(
                    telegram_user_id,
                    "❌ <b>Erro ao carregar grupos</b>\n\nTente fazer login novamente.",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        return []

async def is_user_admin_in_group(client, group, user_id):
    """Verifica se usuário é admin no grupo"""
    try:
        participants = await client.get_participants(group, filter=ChannelParticipantsAdmins)
        admin_ids = {p.id for p in participants}
        return user_id in admin_ids
    except Exception as e:
        logger.warning(f"Erro ao verificar admin status para user_id {user_id}: {e}")
        return False

@antiflood
async def update_groups_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para atualizar grupos"""
    user_id = update.effective_user.id

    if not await user_has_active_payment(user_id):
        await context.bot.send_message(
            user_id,
            "❌ Você não possui um plano ativo."
        )
        return

    async with shared.AsyncSessionLocal() as session:
        stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not (user and user.is_authenticated):
            await context.bot.send_message(
                user_id,
                "🚫 Faça login primeiro para atualizar os grupos."
            )
            return

        client = await get_user_client(user_id, session)
        if not client:
            await context.bot.send_message(
                user_id,
                "❌ Erro ao acessar sua conta. Faça login novamente."
            )
            return

        await context.bot.send_message(user_id, "🔄 <b>Atualizando lista de grupos...</b>", parse_mode="HTML")
        await preload_groups_for_user(user_id, client, context)

# Sistema de autenticação

def login_active_for_user(user_id):
    """Verifica se usuário está em processo de login"""
    return user_id in active_login_conversations

def reset_login_state(user_id):
    """Reseta estado de login do usuário"""
    if user_id in active_login_conversations:
        active_login_conversations.remove(user_id)

async def cancel_and_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancela operação e volta ao menu"""
    if hasattr(update, "effective_user"):
        user_id = update.effective_user.id
        if user_id in active_login_conversations:
            active_login_conversations.remove(user_id)
        await send_menu(user_id, context)
    return ConversationHandler.END

@antiflood
async def start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Inicia processo de autenticação"""
    user_id = update.effective_user.id

    # Resetar estado anterior se existir
    if user_id in active_login_conversations:
        active_login_conversations.remove(user_id)

    active_login_conversations.add(user_id)

    # Verificar se já tem conta conectada
    async with shared.AsyncSessionLocal() as session:
        stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if user and user.is_authenticated:
            keyboard = [
                [
                    InlineKeyboardButton("✅ Sim, substituir", callback_data="replace_session:yes"),
                    InlineKeyboardButton("❌ Não, manter", callback_data="replace_session:no")
                ]
            ]

            message_text = (
                "⚠️ <b>Conta já conectada!</b>\n\n"
                "🔗 Já existe uma conta Telethon conectada ao seu perfil.\n\n"
                "❓ <b>Deseja substituir pela nova conta?</b>\n\n"
                "💡 <i>Substituir irá desconectar a conta atual.</i>"
            )

            if update.callback_query:
                await update.callback_query.message.reply_text(
                    message_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            else:
                await update.message.reply_text(
                    message_text,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(keyboard)
                )
            return CONFIRM_SESSION_REPLACE

    # Começar processo de login
    msg = (
        "📱 <b>Login da Conta Telethon</b>\n\n"
        "🔢 Envie seu número de telefone no formato internacional:\n\n"
        "📋 <b>Exemplo:</b> <code>+5511999999999</code>\n\n"
        "💡 <i>Use o mesmo número da sua conta do Telegram.</i>"
    )

    if update.callback_query:
        await update.callback_query.message.reply_text(msg, parse_mode="HTML")
    else:
        await update.message.reply_text(msg, parse_mode="HTML")

    return AUTH_PHONE

async def confirm_replace_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle de confirmação de substituição de sessão"""
    user_id = update.effective_user.id

    if update.callback_query.data == "replace_session:no":
        reset_login_state(user_id)
        await update.callback_query.edit_message_text(
            "✅ <b>Operação cancelada</b>\n\nSua conta atual permanece conectada.",
            parse_mode="HTML"
        )
        await send_menu(user_id, context)
        return ConversationHandler.END

    elif update.callback_query.data == "replace_session:yes":
        # Remover sessão antiga da memória, mas não apagar o arquivo
        async with shared.AsyncSessionLocal() as session:
            stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
            result = await session.execute(stmt)
            user = result.scalar_one_or_none()

            if user:
                # Desconectar cliente se existir, sem apagar arquivo de sessão
                if user_id in shared.user_clients:
                    try:
                        await shared.user_clients[user_id].disconnect()
                    except Exception as e:
                        logger.warning(f"Erro ao desconectar cliente de {user_id}: {e}")
                    del shared.user_clients[user_id]

                # Atualizar banco, mantendo o arquivo de sessão intacto
                user.is_authenticated = False
                await session.commit()
                logger.info(f"Sessão de {user_id} marcada como não autenticada, arquivo de sessão preservado")

        # Limpar cache
        clear_cache(user_id)

        # Reiniciar processo de login
        reset_login_state(user_id)
        active_login_conversations.add(user_id)

        await update.callback_query.edit_message_text(
            "🗑️ <b>Sessão anterior desconectada</b>\n\n"
            "📱 Agora envie seu número de telefone:\n\n"
            "📋 <b>Exemplo:</b> <code>+5511999999999</code>",
            parse_mode="HTML"
        )

        return AUTH_PHONE

@antiflood
async def auth_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para número de telefone"""
    user_id = update.effective_user.id
    user_message = update.message.text.strip()

    # Verificar comando /start
    if user_message == "/start":
        reset_login_state(user_id)
        return await cancel_and_menu(update, context)

    # Validar formato do telefone
    if not re.match(r"^\+\d{10,15}$", user_message):
        await update.message.reply_text(
            "❌ <b>Número inválido!</b>\n\n"
            "📋 <b>Formato correto:</b> <code>+5511999999999</code>\n\n"
            "🔄 Tente novamente ou use /start para cancelar.",
            parse_mode="HTML"
        )
        return AUTH_PHONE

    context.user_data["phone"] = user_message
    update_cache(user_id, "phone", user_message)

    await update.message.reply_text(
        "✅ <b>Número salvo!</b>\n\n"
        "🔑 Agora envie seu <b>API ID</b>:\n\n"
        "💡 <i>Você obtém isso em https://my.telegram.org</i>",
        parse_mode="HTML"
    )

    return AUTH_API_ID

@antiflood
async def auth_api_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para API ID"""
    user_id = update.effective_user.id

    if update.message.text == "/start":
        reset_login_state(user_id)
        return await cancel_and_menu(update, context)

    api_id = update.message.text.strip()

    if not api_id.isdigit() or len(api_id) < 6:
        await update.message.reply_text(
            "❌ <b>API ID inválido!</b>\n\n"
            "📋 <b>Requisitos:</b>\n"
            "• Apenas números\n"
            "• Mínimo 6 dígitos\n\n"
            "💡 <i>Obtenha em https://my.telegram.org</i>",
            parse_mode="HTML"
        )
        return AUTH_API_ID

    context.user_data["api_id"] = api_id
    update_cache(user_id, "api_id", api_id)

    await update.message.reply_text(
        "✅ <b>API ID salvo!</b>\n\n"
        "🔐 Agora envie seu <b>API HASH</b>:\n\n"
        "💡 <i>String de 32 caracteres do my.telegram.org</i>",
        parse_mode="HTML"
    )

    return AUTH_API_HASH

@antiflood
async def auth_api_hash(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para API Hash"""
    user_id = update.effective_user.id

    if update.message.text == "/start":
        reset_login_state(user_id)
        return await cancel_and_menu(update, context)

    api_hash = update.message.text.strip()

    if not re.match(r"^[a-zA-Z0-9]{32}$", api_hash):
        await update.message.reply_text(
            "❌ <b>API Hash inválido!</b>\n\n"
            "📋 <b>Requisitos:</b>\n"
            "• Exatamente 32 caracteres\n"
            "• Apenas letras e números\n\n"
            "💡 <i>Copie exatamente do my.telegram.org</i>",
            parse_mode="HTML"
        )
        return AUTH_API_HASH

    context.user_data["api_hash"] = api_hash
    update_cache(user_id, "api_hash", api_hash)

    # Enviar código de verificação
    await update.message.reply_text("📲 <b>Enviando código de verificação...</b>", parse_mode="HTML")

    phone = context.user_data["phone"]
    api_id = context.user_data["api_id"]
    session_path = get_session_path(user_id, phone)

    try:
        client = TelegramClient(session_path, api_id, api_hash, timeout=CLIENT_TIMEOUT)
        await client.connect()

        sent = await client.send_code_request(phone)
        context.user_data["client"] = client
        context.user_data["session_path"] = session_path
        update_cache(user_id, "session_path", session_path)

        await update.message.reply_text(
            "✉️ <b>Código enviado!</b>\n\n"
            "📱 Verifique seu Telegram e envie o código recebido:\n\n"
            "📋 <b>Formato:</b> <code>12345 ou 1,2,3,4,5 ou bot1,2,3,4,5</code>\n\n"
            "⏰ <i>O código expira em alguns minutos.</i>",
            parse_mode="HTML"
        )

        return AUTH_CODE

    except PhoneNumberBannedError:
        await update.message.reply_text(
            "🚫 <b>Número banido</b>\n\n"
            "❌ Este número está banido do Telegram.\n\n"
            "💡 Use outro número ou contate o suporte do Telegram.",
            parse_mode="HTML"
        )

        await client.disconnect()
        reset_login_state(user_id)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Erro ao enviar código para {user_id}: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ <b>Erro ao enviar código</b>\n\n"
            "🔄 Verifique seus dados e tente novamente.\n\n"
            "💡 Certifique-se de que API ID e Hash estão corretos.",
            parse_mode="HTML"
        )

        await client.disconnect()
        reset_login_state(user_id)
        return ConversationHandler.END

def parse_code(text):
    """Parse do código de verificação para aceitar formatos como 1,2,3,4,5 ou bot1,2,3,4,5"""
    text = text.strip().lower()
    if text.startswith('bot'):
        text = text[3:]
    digits = [d.strip() for d in text.split(',') if d.strip().isdigit()]
    return ''.join(digits)

@antiflood
async def auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para código de verificação"""
    user_id = update.effective_user.id

    if update.message.text == "/start":
        reset_login_state(user_id)
        return await cancel_and_menu(update, context)

    code = parse_code(update.message.text)

    # Validar formato do código
    if not code.isdigit() or len(code) != 5:
        await update.message.reply_text(
            "❌ <b>Código inválido!</b>\n\n"
            "📋 <b>Formato:</b> 5 dígitos (ex: 12345 ou 1,2,3,4,5 ou bot1,2,3,4,5)\n\n"
            "🔄 Tente novamente.",
            parse_mode="HTML"
        )
        return AUTH_CODE

    client = context.user_data.get("client")
    phone = context.user_data.get("phone")
    api_id = context.user_data.get("api_id")
    api_hash = context.user_data.get("api_hash")
    session_path = context.user_data.get("session_path")

    try:
        await client.sign_in(phone, code)
    except SessionPasswordNeededError:
        await update.message.reply_text(
            "🔒 <b>Verificação em 2 etapas ativa</b>\n\n"
            "🔐 Sua conta tem autenticação de 2 fatores.\n\n"
            "🔑 Envie sua senha de proteção:",
            parse_mode="HTML"
        )
        return AUTH_PASSWORD

    except PhoneCodeInvalidError:
        await update.message.reply_text(
            "❌ <b>Código inválido</b>\n\n"
            "🔄 Verifique o código e tente novamente.\n\n"
            "💡 O código expira em alguns minutos.",
            parse_mode="HTML"
        )
        return AUTH_CODE

    except Exception as e:
        logger.error(f"Erro na autenticação para {user_id}: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ <b>Erro na autenticação</b>\n\n"
            "🔄 Código pode ter expirado. Tente fazer login novamente.",
            parse_mode="HTML"
        )

        await client.disconnect()
        reset_login_state(user_id)
        return ConversationHandler.END

    # Sucesso na autenticação
    await save_authenticated_user(user_id, phone, api_id, api_hash, session_path, client)

    await update.message.reply_text(
        "✅ <b>Login realizado com sucesso!</b>\n\n"
        "🎉 Sua conta Telethon foi conectada.\n\n"
        "🚀 Agora você pode usar todas as funções do bot!",
        parse_mode="HTML"
    )

    reset_login_state(user_id)
    return ConversationHandler.END

@antiflood
async def auth_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para senha 2FA"""
    user_id = update.effective_user.id

    if update.message.text == "/start":
        reset_login_state(user_id)
        return await cancel_and_menu(update, context)

    password = update.message.text.strip()
    client = context.user_data.get("client")
    phone = context.user_data.get("phone")
    api_id = context.user_data.get("api_id")
    api_hash = context.user_data.get("api_hash")
    session_path = context.user_data.get("session_path")

    try:
        await client.sign_in(password=password)

    except Exception as e:
        logger.error(f"Erro na senha 2FA para {user_id}: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ <b>Senha incorreta</b>\n\n"
            "🔄 Tente novamente ou use /start para cancelar.\n\n"
            "💡 Use a senha de verificação em 2 etapas do Telegram.",
            parse_mode="HTML"
        )
        return AUTH_PASSWORD

    # Sucesso na autenticação
    await save_authenticated_user(user_id, phone, api_id, api_hash, session_path, client)

    await update.message.reply_text(
        "✅ <b>Login realizado com sucesso!</b>\n\n"
        "🎉 Sua conta Telethon foi conectada com 2FA.\n\n"
        "🚀 Agora você pode usar todas as funções do bot!",
        parse_mode="HTML"
    )

    reset_login_state(user_id)
    return ConversationHandler.END

async def save_authenticated_user(user_id, phone, api_id, api_hash, session_path, client):
    """Salva usuário autenticado no banco"""
    async with shared.AsyncSessionLocal() as session:
        stmt = select(shared.User).where(shared.User.telegram_user_id == user_id)
        result = await session.execute(stmt)
        user = result.scalar_one_or_none()

        if not user:
            user = shared.User(
                telegram_user_id=user_id,
                phone=phone,
                api_id=api_id,
                api_hash=api_hash,
                session_path=session_path,
                is_authenticated=True,
                is_banned=False
            )
            session.add(user)
        else:
            user.phone = phone
            user.api_id = api_id
            user.api_hash = api_hash
            user.session_path = session_path
            user.is_authenticated = True
            user.is_banned = False

        await session.commit()

    # Salvar cliente na memória
    shared.user_clients[user_id] = client
    logger.info(f"Usuário {user_id} autenticado com sucesso")

# Handlers de menu

async def inline_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler para voltar ao menu"""
    await send_menu(update.effective_user.id, context)

# Handler de erros

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Handler global de erros"""
    logger.error("Erro não tratado:", exc_info=context.error)
    error_counter.labels(error_type='unhandled').inc()

    if hasattr(update, "effective_user") and update.effective_user:
        try:
            await context.bot.send_message(
                update.effective_user.id,
                "❌ <b>Ocorreu um erro inesperado</b>\n\n"
                "🔄 Tente novamente ou entre em contato com o suporte se o problema persistir.",
                parse_mode="HTML"
            )
        except Exception:
            pass

# Utilitários de verificação

async def verificar_planos_para_expirar(bot):
    """Verifica e notifica sobre planos próximos do vencimento"""
    try:
        from datetime import date
        async with shared.AsyncSessionLocal() as session:
            # Planos que expiram em 1 dia
            stmt = select(Payment).where(
                Payment.status == "approved",
                Payment.expires_at == date.today() + timedelta(days=1)
            )
            result = await session.execute(stmt)
            pagamentos = result.scalars().all()

            for pagamento in pagamentos:
                try:
                    await bot.send_message(
                        chat_id=pagamento.telegram_user_id,
                        text=(
                            f"⚠️ <b>Seu plano expira amanhã!</b>\n\n"
                            f"📅 <b>Data de expiração:</b> {pagamento.expires_at.strftime('%d/%m/%Y')}\n\n"
                            f"💡 Renove seu acesso para continuar usando o bot.\n\n"
                            f"📦 Use o menu para escolher um novo plano."
                        ),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            # Planos que expiraram hoje
            stmt = select(Payment).where(
                Payment.status == "approved",
                Payment.expires_at == date.today()
            )
            result = await session.execute(stmt)
            pagamentos_expirados = result.scalars().all()

            for pagamento in pagamentos_expirados:
                try:
                    await bot.send_message(
                        chat_id=pagamento.telegram_user_id,
                        text=(
                            f"⏰ <b>Seu plano expirou hoje!</b>\n\n"
                            f"❌ O acesso às funções do bot foi suspenso.\n\n"
                            f"💡 Adquira um novo plano para continuar usando o bot.\n\n"
                            f"📦 Use /start para acessar os planos disponíveis."
                        ),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            logger.info(f"Verificação de expiração: {len(pagamentos)} avisos, {len(pagamentos_expirados)} expirados")

    except Exception as e:
        logger.error(f"Erro na verificação de planos: {e}", exc_info=True)

# Função para reiniciar campanhas ativas após reinicialização
async def restart_active_campaigns():
    """Reinicia campanhas ativas salvas no cache ou banco"""
    try:
        logger.info("Verificando campanhas ativas para reinicialização...")
        for user_id in list(shared.active_campaigns.keys()):
            if has_active_campaign(user_id):
                campaign_data = shared.active_campaigns[user_id]
                job_id = campaign_data.get('job_id')
                interval = campaign_data.get('interval')
                if job_id:
                    try:
                        # Verificar se o job ainda existe na fila
                        job = shared.rq_queue.get_job(job_id)
                        if not job:
                            logger.warning(f"Job {job_id} não encontrado para usuário {user_id}, reiniciando campanha")
                            delay = timedelta(minutes=interval)
                            new_job = shared.rq_queue.enqueue_in(delay, forward_message_com_RQ, user_id)
                            shared.active_campaigns[user_id]['job_id'] = new_job.id
                            logger.info(f"Novo job {new_job.id} criado para usuário {user_id}")
                    except Exception as e:
                        logger.error(f"Erro ao verificar job {job_id} para {user_id}: {e}", exc_info=True)
                        delay = timedelta(minutes=interval)
                        new_job = shared.rq_queue.enqueue_in(delay, forward_message_com_RQ, user_id)
                        shared.active_campaigns[user_id]['job_id'] = new_job.id
                        logger.info(f"Novo job {new_job.id} criado para usuário {user_id} após erro")
        shared.statistics['active_campaigns'] = len(shared.active_campaigns)
        shared.active_campaigns_gauge.set(len(shared.active_campaigns))
        logger.info(f"Reinicialização de campanhas concluída: {len(shared.active_campaigns)} campanhas ativas")
    except Exception as e:
        logger.error(f"Erro ao reiniciar campanhas ativas: {e}", exc_info=True)

Função principal

# Função principal
async def main():
    """Função principal do bot"""
    try:
        # Inicializar banco de dados
        await init_db()
        # Configurar aplicação
        application = (
            ApplicationBuilder()
            .token(BOT_TOKEN)
            .connect_timeout(30)
            .read_timeout(60)
            .write_timeout(30)
            .pool_timeout(20)
            .concurrent_updates(MAX_CONCURRENT_OPERATIONS)
            .build()
        )

        # Iniciar servidor Prometheus  
        try:  
            start_http_server(2222, registry=PROM_REGISTRY)  
            logger.info("Servidor de métricas iniciado na porta 2222")  
        except Exception as e:  
            logger.error(f"Erro ao iniciar servidor de métricas: {e}")  

        # Task para verificação de planos  
        async def loop_expiracao():  
            while True:  
                await verificar_planos_para_expirar(application.bot)  
                await asyncio.sleep(3600 * 12)  # A cada 12 horas  

        asyncio.create_task(loop_expiracao())

        # Executar polling do bot
        await application.run_polling()

    except Exception as e:
        logger.critical(f"Erro crítico na inicialização: {e}", exc_info=True)
        raise


        # Task para limpeza de cache
        asyncio.create_task(cache_cleanup_job())

        # Reiniciar campanhas ativas
        await restart_active_campaigns()

        # Handlers principais
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("cancel", cancel_campaign))

        # Menu de pagamentos
        application.add_handler(CallbackQueryHandler(payment_menu, pattern='^payment_menu$'))
        application.add_handler(CallbackQueryHandler(payment_button_handler, pattern='^pay_'))

        # Conversation handler para login
        login_conv_handler = ConversationHandler(
            entry_points=[CallbackQueryHandler(start_auth, pattern='^login$')],
            states={
                AUTH_PHONE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, auth_phone),
                    MessageHandler(filters.Regex(r"^/start$"), cancel_and_menu)
                ],
                AUTH_API_ID: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, auth_api_id),
                    MessageHandler(filters.Regex(r"^/start$"), cancel_and_menu)
                ],
                AUTH_API_HASH: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, auth_api_hash),
                    MessageHandler(filters.Regex(r"^/start$"), cancel_and_menu)
                ],
                AUTH_CODE: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, auth_code),
                    MessageHandler(filters.Regex(r"^/start$"), cancel_and_menu)
                ],
                AUTH_PASSWORD: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, auth_password),
                    MessageHandler(filters.Regex(r"^/start$"), cancel_and_menu)
                ],
                CONFIRM_SESSION_REPLACE: [
                    CallbackQueryHandler(confirm_replace_callback, pattern='^replace_session:')
                ]
            },
            fallbacks=[CommandHandler('cancel', cancel_and_menu)],
            name="login_conv_handler",
            persistent=False
        )
        application.add_handler(login_conv_handler)

        # Conversation handler para campanhas
        campaign_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(start_campaign, pattern='^create_campaign$')],
            states={
                LINK: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_message_link)],
                INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_interval)],
            },
            fallbacks=[CommandHandler('cancel', cancel_campaign)],
            name="campaign_conv_handler",
            persistent=False
        )
        application.add_handler(campaign_conv)

        # Outros handlers
        application.add_handler(CallbackQueryHandler(cancel_campaign, pattern='^cancel_campaign$'))
        application.add_handler(CallbackQueryHandler(show_statistics, pattern='^statistics$'))
        application.add_handler(CallbackQueryHandler(update_groups_handler, pattern='^update_groups$'))
        application.add_handler(CallbackQueryHandler(inline_menu_handler, pattern='^main_menu$'))

        # Handler genérico para callback queries
        application.add_handler(CallbackQueryHandler(inline_menu_handler))

        # Handler de erros
        application.add_error_handler(error_handler)

        # Log de inicialização
        print("\n" + "="*50)
        print("🚀 BOT TELEGRAM INICIADO COM SUCESSO")
        print(f"👤 Admin: {ADMIN_USERNAME}")
        print(f"🆔 Owner ID: {OWNER_ID}")
        print("📡 Servidor: ✅ Operacional")
        print(f"💾 Banco: {'PostgreSQL' if 'postgres' in DATABASE_URL else 'SQLite'} ✅")
        print("📈 Métricas: http://localhost:2222 ✅")
        print(f"⚡ Redis: {REDIS_HOST}:{REDIS_PORT} ✅")
        print(f"⚙️ Max operações simultâneas: {MAX_CONCURRENT_OPERATIONS}")
        print("="*50 + "\n")

        # Iniciar job de validação de sessões
        asyncio.create_task(session_validation_job(application))

        # Iniciar polling
        logger.info("Iniciando polling do bot...")
        await application.run_polling()
    except Exception as e:
        logger.critical(f"Erro crítico na inicialização: {e}", exc_info=True)
        raise

# Executar apenas se for o arquivo principal
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot interrompido pelo usuário")
    except Exception as e:
        logger.critical(f"Erro fatal: {e}", exc_info=True)
        raise
else:
    # Quando importado pelo worker
    logger.info("Módulo v4.py importado - inicializando dependências...")

    async def _init():
        await init_db()

    try:
        asyncio.run(_init())
    except Exception as e:
        logger.critical(f"Erro ao inicializar dependências: {e}", exc_info=True)
        raise

    # Configurar variáveis globais para o worker
    shared.AsyncSessionLocal = AsyncSessionLocal
    shared.rq_queue = rq_queue
    shared.User = User
    logger.info("Dependências do worker configuradas com sucesso")
