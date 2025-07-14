#!/bin/bash

# ===== CONFIGURAÇÕES =====
NUM_WORKERS=${1:-3}  # Número padrão de workers
MAX_WORKERS=10        # Limite máximo de workers

# Cores
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🔄 Iniciando Sistema Multi-Worker${NC}"
echo -e "${BLUE}════════════════════════════════════${NC}"

# Validar número de workers
if [ "$NUM_WORKERS" -gt "$MAX_WORKERS" ]; then
    echo -e "${RED}❌ Máximo de $MAX_WORKERS workers permitidos${NC}"
    exit 1
fi

if [ "$NUM_WORKERS" -lt 1 ]; then
    echo -e "${RED}❌ Mínimo de 1 worker necessário${NC}"
    exit 1
fi

# Verificar Redis
echo -e "${YELLOW}🔍 Verificando Redis...${NC}"
redis-cli ping >/dev/null 2>&1 || {
    echo -e "${RED}❌ Redis não está disponível${NC}"
    echo -e "${YELLOW}💡 Execute: sudo systemctl start redis${NC}"
    exit 1
}
echo -e "${GREEN}✅ Redis OK${NC}"

# Verificar ambiente virtual
if [ ! -f "venv/bin/activate" ]; then
    echo -e "${RED}❌ Ambiente virtual não encontrado${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Ambiente virtual OK${NC}"

# Parar workers existentes
echo -e "${YELLOW}🛑 Parando workers existentes...${NC}"
pkill -f "python.*worker.py" 2>/dev/null
sleep 2

# Verificar se pararam
if pgrep -f "python.*worker.py" >/dev/null; then
    echo -e "${YELLOW}⚠️ Forçando parada de workers...${NC}"
    pkill -9 -f "python.*worker.py" 2>/dev/null
    sleep 1
fi

# Criar diretórios
mkdir -p logs pids

# Função para iniciar worker
start_worker() {
    local worker_id=$1
    local worker_name="worker-$worker_id"
    
    echo -e "${BLUE}🚀 Iniciando $worker_name...${NC}"
    
    # Executar worker em background
    WORKER_NAME="$worker_name" WORKER_ID="$worker_id" \
    nohup bash start_worker.sh > logs/${worker_name}.log 2>&1 &
    
    local pid=$!
    echo $pid > pids/${worker_name}.pid
    
    # Verificar se iniciou corretamente
    sleep 1
    if kill -0 $pid 2>/dev/null; then
        echo -e "${GREEN}✅ $worker_name iniciado (PID: $pid)${NC}"
        return 0
    else
        echo -e "${RED}❌ Falha ao iniciar $worker_name${NC}"
        return 1
    fi
}

# Iniciar workers
echo -e "${BLUE}🔄 Iniciando $NUM_WORKERS workers...${NC}"
workers_started=0

for i in $(seq 1 $NUM_WORKERS); do
    if start_worker $i; then
        workers_started=$((workers_started + 1))
    fi
    
    # Pequena pausa entre workers
    sleep 2
done

echo -e "${BLUE}════════════════════════════════════${NC}"
echo -e "${GREEN}✅ $workers_started/$NUM_WORKERS workers iniciados${NC}"

# Status final
echo -e "${BLUE}📊 Status do Sistema:${NC}"
echo -e "${YELLOW}• Workers ativos:${NC} $(pgrep -f 'python.*worker.py' | wc -l)"
echo -e "${YELLOW}• PIDs:${NC} $(pgrep -f 'python.*worker.py' | tr '\n' ' ')"
echo -e "${YELLOW}• Logs:${NC} logs/worker-*.log"

# Comandos úteis
echo -e "${BLUE}💡 Comandos úteis:${NC}"
echo -e "${YELLOW}• Monitorar logs:${NC} tail -f logs/worker-*.log"
echo -e "${YELLOW}• Parar workers:${NC} bash stop_workers.sh"
echo -e "${YELLOW}• Status Redis:${NC} redis-cli info | grep connected_clients"

# Opcional: monitoramento contínuo
read -p "Deseja monitorar os workers em tempo real? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}📊 Monitoramento ativo (Ctrl+C para sair)...${NC}"
    while true; do
        clear
        echo -e "${BLUE}═══ MONITORAMENTO WORKERS - $(date) ═══${NC}"
        echo -e "${YELLOW}Workers ativos:${NC} $(pgrep -f 'python.*worker.py' | wc -l)/$NUM_WORKERS"
        echo -e "${YELLOW}Uso de CPU:${NC} $(ps -o pid,pcpu,pmem,cmd -p $(pgrep -f 'python.*worker.py' | tr '\n' ' ') 2>/dev/null || echo 'Nenhum worker ativo')"
        echo ""
        echo -e "${YELLOW}Filas Redis:${NC}"
        redis-cli LLEN rq:queue:bot_tasks 2>/dev/null | xargs echo "• bot_tasks:"
        redis-cli LLEN rq:queue:high_priority 2>/dev/null | xargs echo "• high_priority:"
        redis-cli LLEN rq:queue:low_priority 2>/dev/null | xargs echo "• low_priority:"
        echo ""
        echo -e "${BLUE}Pressione Ctrl+C para sair do monitoramento${NC}"
        sleep 5
    done
fi
