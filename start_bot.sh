#!/bin/bash

# ===== CONFIGURAÇÕES =====
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="logs/startup.log"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função de log
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
    echo "[ERROR] $1" >> "$LOG_FILE"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    echo "[SUCCESS] $1" >> "$LOG_FILE"
}

warn() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    echo "[WARNING] $1" >> "$LOG_FILE"
}

# ===== INÍCIO DO SCRIPT =====
log "🚀 Iniciando Bot Telegram Otimizado..."

# Verificar se está no diretório correto
cd "$PROJECT_DIR" || {
    error "❌ Não foi possível acessar o diretório do projeto"
    exit 1
}

# Criar diretórios necessários
mkdir -p logs sessions backups
log "📁 Diretórios criados/verificados"

# Ativar ambiente virtual
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    success "✅ Ambiente virtual ativado"
else
    error "❌ Ambiente virtual não encontrado em venv/bin/activate"
    exit 1
fi

# Verificar arquivo .env
if [ ! -f ".env" ]; then
    error "❌ Arquivo .env não encontrado"
    error "📝 Crie o arquivo .env baseado no .env.example"
    exit 1
fi

# Verificar e instalar dependências críticas
log "📦 Verificando e instalando dependências..."
python -c "
import sys
try:
    import redis, sqlalchemy, telegram, telethon, rq, nest_asyncio, dotenv, prometheus_client, msgpack, mercadopago, requests
    print('✅ Dependências principais OK')
except ImportError as e:
    print(f'❌ Dependência faltando: {e}')
    sys.exit(1)
" || {
    warn "⚠️ Algumas dependências estão faltando. Instalando agora..."
    pip install -r requirements.txt || {
        error "❌ Falha ao instalar dependências. Verifique requirements.txt"
        exit 1
    }
    # Instalação explícita de nest-asyncio caso requirements.txt não o inclua
    pip install nest-asyncio || {
        error "❌ Falha ao instalar nest-asyncio"
        exit 1
    }
    # Instalação explícita de requests caso requirements.txt não o inclua
    pip install requests || {
        error "❌ Falha ao instalar requests"
        exit 1
    }
    success "✅ Dependências instaladas com sucesso"
}

# Verificar conexão Redis
log "🔍 Verificando conexão Redis..."
if command -v redis-cli >/dev/null 2>&1; then
    if redis-cli ping >/dev/null 2>&1; then
        success "✅ Redis conectado"
    else
        error "❌ Redis não está respondendo"
        error "🔧 Execute: sudo systemctl start redis"
        exit 1
    fi
else
    warn "⚠️ redis-cli não encontrado, mas continuando..."
fi

# Verificar portas
log "🔌 Verificando portas..."
check_port() {
    local port=$1
    local service=$2
    if netstat -tuln 2>/dev/null | grep ":$port " >/dev/null; then
        warn "⚠️ Porta $port já está em uso ($service)"
    else
        log "✅ Porta $port disponível ($service)"
    fi
}
check_port 2222 "Métricas Prometheus"

# Backup do banco antes de iniciar (se existir)
if [ -f "meubanco.db" ]; then
    log "💾 Criando backup do banco..."
    cp meubanco.db "backups/meubanco_backup_$(date +%Y%m%d_%H%M%S).db"
    success "✅ Backup criado"
fi

# Verificar se já existe processo rodando
if pgrep -f "python.*v4.py" >/dev/null; then
    warn "⚠️ Bot já está rodando. PID: $(pgrep -f 'python.*v4.py')"
    read -p "Deseja parar o processo atual e reiniciar? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log "🛑 Parando processo atual..."
        pkill -f "python.*v4.py"
        sleep 3
    else
        log "❌ Cancelado pelo usuário"
        exit 0
    fi
fi

# Iniciar bot
log "🤖 Iniciando bot principal..."
log "📊 Métricas estarão disponíveis em: http://localhost:2222/metrics"
log "📋 Logs em tempo real: tail -f logs/bot.log"

# Executar bot com tratamento de erro
python v4.py 2>&1 | tee -a logs/bot.log
exit_code=$?

if [ $exit_code -eq 0 ]; then
    success "✅ Bot encerrado normalmente"
else
    error "❌ Bot encerrado com erro (código: $exit_code)"
    error "📋 Verifique os logs para mais detalhes"
fi

exit $exit_code
