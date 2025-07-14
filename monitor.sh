#!/bin/bash

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Função para mostrar status
show_status() {
    clear
    echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}        🤖 MONITOR DO BOT TELEGRAM OTIMIZADO        ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
    echo ""
    
    # Data/Hora
    echo -e "${YELLOW}📅 $(date '+%Y-%m-%d %H:%M:%S')${NC}"
    echo ""
    
    # Status dos processos
    echo -e "${BLUE}🔄 PROCESSOS:${NC}"
    
    # Bot principal
    if pgrep -f "python.*v4.py" >/dev/null; then
        local bot_pid=$(pgrep -f "python.*v4.py")
        echo -e "${GREEN}✅ Bot Principal: RODANDO (PID: $bot_pid)${NC}"
    else
        echo -e "${RED}❌ Bot Principal: PARADO${NC}"
    fi
    
    # Workers
    local worker_count=$(pgrep -f "python.*worker.py" | wc -l)
    if [ "$worker_count" -gt 0 ]; then
        echo -e "${GREEN}✅ Workers: $worker_count ATIVOS${NC}"
        echo -e "${YELLOW}   PIDs: $(pgrep -f 'python.*worker.py' | tr '\n' ' ')${NC}"
    else
        echo -e "${RED}❌ Workers: NENHUM ATIVO${NC}"
    fi
    
    echo ""
    
    # Redis Status
    echo -e "${BLUE}📦 REDIS:${NC}"
    if redis-cli ping >/dev/null 2>&1; then
        local redis_clients=$(redis-cli info clients 2>/dev/null | grep connected_clients | cut -d: -f2 | tr -d '\r')
        local redis_memory=$(redis-cli info memory 2>/dev/null | grep used_memory_human | cut -d: -f2 | tr -d '\r')
        echo -e "${GREEN}✅ Status: CONECTADO${NC}"
        echo -e "${YELLOW}   Clientes: $redis_clients${NC}"
        echo -e "${YELLOW}   Memória: $redis_memory${NC}"
    else
        echo -e "${RED}❌ Status: DESCONECTADO${NC}"
    fi
    
    echo ""
    
    # Filas
    echo -e "${BLUE}📋 FILAS:${NC}"
    if redis-cli ping >/dev/null 2>&1; then
        local main_queue=$(redis-cli LLEN rq:queue:bot_tasks 2>/dev/null || echo "0")
        local high_queue=$(redis-cli LLEN rq:queue:high_priority 2>/dev/null || echo "0")
        local low_queue=$(redis-cli LLEN rq:queue:low_priority 2>/dev/null || echo "0")
        
        echo -e "${YELLOW}   bot_tasks: $main_queue${NC}"
        echo -e "${YELLOW}   high_priority: $high_queue${NC}"
        echo -e "${YELLOW}   low_priority: $low_queue${NC}"
    else
        echo -e "${RED}   Não foi possível consultar as filas${NC}"
    fi
    
    echo ""
    
    # Uso de recursos
    echo -e "${BLUE}💻 RECURSOS:${NC}"
    if command -v free >/dev/null 2>&1; then
        local memory_usage=$(free | awk 'FNR==2{printf "%.1f%%", $3/($3+$4)*100}')
        echo -e "${YELLOW}   Memória: $memory_usage${NC}"
    fi
    
    if command -v df >/dev/null 2>&1; then
        local disk_usage=$(df -h . | awk 'FNR==2{print $5}')
        echo -e "${YELLOW}   Disco: $disk_usage${NC}"
    fi
    
    # Load average (Linux/Mac)
    if [ -f /proc/loadavg ]; then
        local load_avg=$(cat /proc/loadavg | cut -d' ' -f1-3)
        echo -e "${YELLOW}   Load: $load_avg${NC}"
    fi
    
    echo ""
    
    # Logs recentes
    echo -e "${BLUE}📝 LOGS RECENTES:${NC}"
    if [ -f "logs/bot.log" ]; then
        echo -e "${YELLOW}   Bot (últimas 3 linhas):${NC}"
        tail -n 3 logs/bot.log 2>/dev/null | sed 's/^/      /'
    fi
    
    if [ -f "logs/worker.log" ] || ls logs/worker-*.log >/dev/null 2>&1; then
        echo -e "${YELLOW}   Worker (últimas 2 linhas):${NC}"
        (ls logs/worker*.log 2>/dev/null | head -n 1 | xargs tail -n 2 2>/dev/null || echo "      Nenhum log encontrado") | sed 's/^/      /'
    fi
    
    echo ""
    echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
    echo -e "${YELLOW}⚡ Atualização automática a cada 5 segundos${NC}"
    echo -e "${YELLOW}🔄 Pressione Ctrl+C para sair${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════${NC}"
}

# Loop principal
echo -e "${BLUE}🚀 Iniciando monitor...${NC}"
sleep 2

trap 'echo -e "\n${GREEN}👋 Monitor encerrado${NC}"; exit 0' INT

while true; do
    show_status
    sleep 5
done
