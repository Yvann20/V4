#!/bin/bash

# ===== CONFIGURAÇÕES =====
WORKER_ID=${WORKER_ID:-$(date +%s)}
WORKER_NAME="worker-$WORKER_ID"
LOG_DIR="logs"
LOG_FILE="$LOG_DIR/worker_$WORKER_ID.log"
VENVPATH="venv/bin/activate"  # Ajuste o caminho do ambiente virtual se necessário
PROJECT_DIR="/home/ubuntu/superbot"  # Ajuste o diretório do projeto se necessário
REDIS_HOST=${REDIS_HOST:-"localhost"}
REDIS_PORT=${REDIS_PORT:-6379}

# Cores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERRO: $1${NC}"
}

success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✅ SUCESSO: $1${NC}"
}

warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️ AVISO: $1${NC}"
}

# Criar diretórios de logs se não existirem
mkdir -p "$LOG_DIR" || {
    error "Não foi possível criar diretório de logs: $LOG_DIR"
    exit 1
}

# Função para verificar e parar workers antigos
stop_old_workers() {
    log "Verificando workers antigos para parar..."
    PIDS=$(ps aux | grep -E "rq.worker|python.*worker" | grep -v "grep" | awk '{print $2}')
    if [ -n "$PIDS" ]; then
        warning "Workers antigos encontrados (PIDs: $PIDS). Parando..."
        for PID in $PIDS; do
            sudo kill -9 "$PID" 2>/dev/null
            if [ $? -eq 0 ]; then
                log "Worker antigo (PID: $PID) parado com sucesso."
            else
                warning "Falha ao parar worker antigo (PID: $PID)."
            fi
        done
    else
        log "Nenhum worker antigo encontrado. Prosseguindo..."
    fi
}

# Limpar cache de módulos Python para evitar conflitos
log "Limpando cache de módulos Python..."
find "$PROJECT_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
success "Cache de módulos limpo."

# Verificar ambiente virtual
log "Ativando ambiente virtual..."
source "$VENVPATH" || {
    error "Erro ao ativar ambiente virtual em $VENVPATH. Verifique o caminho."
    exit 1
}
success "Ambiente virtual ativado."

# Verificar Redis
log "Verificando conexão com Redis ($REDIS_HOST:$REDIS_PORT)..."
redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping >/dev/null 2>&1 || {
    error "Redis não disponível em $REDIS_HOST:$REDIS_PORT. Verifique se o serviço está rodando."
    exit 1
}
success "Redis disponível e respondendo."

# Mudar para o diretório do projeto
log "Mudando para o diretório do projeto: $PROJECT_DIR..."
cd "$PROJECT_DIR" || {
    error "Não foi possível mudar para o diretório do projeto: $PROJECT_DIR"
    exit 1
}
success "Diretório do projeto acessado."

# Parar workers antigos antes de iniciar um novo
stop_old_workers

# Definir nome do worker
export WORKER_NAME="$WORKER_NAME"

log "⚙️ Iniciando worker: $WORKER_NAME"
log "📋 Logs serão salvos em: $LOG_FILE"

# Iniciar worker com comando simplificado
nohup python -m rq.worker bot_tasks 2>&1 | tee "$LOG_FILE" &

# Capturar o PID do worker
WORKER_PID=$!
sleep 2  # Aguardar um momento para verificar se o worker iniciou

# Verificar se o worker está rodando
if ps -p "$WORKER_PID" > /dev/null; then
    success "Worker iniciado com sucesso (PID: $WORKER_PID)"
else
    error "Falha ao iniciar o worker. Verifique os logs em $LOG_FILE"
    exit 1
fi

log "🛠️ Para verificar logs em tempo real, use: tail -f $LOG_FILE"
log "🛑 Para parar o worker, use: sudo kill -9 $WORKER_PID"
