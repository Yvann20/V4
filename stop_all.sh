#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}🛑 Parando Sistema Completo...${NC}"

# Parar bot principal
echo -e "${YELLOW}📱 Parando bot principal...${NC}"
pkill -f "python.*v4.py"

# Parar workers
echo -e "${YELLOW}⚙️ Parando workers...${NC}"
pkill -f "python.*worker.py"

# Aguardar processos pararem graciosamente
echo -e "${YELLOW}⏳ Aguardando 5 segundos...${NC}"
sleep 5

# Verificar e forçar se necessário
if pgrep -f "python.*(v4|worker).py" >/dev/null; then
    echo -e "${YELLOW}⚠️ Forçando parada de processos restantes...${NC}"
    pkill -9 -f "python.*(v4|worker).py"
    sleep 2
fi

# Limpar arquivos PID
rm -f pids/*.pid 2>/dev/null

# Status final
if pgrep -f "python.*(v4|worker).py" >/dev/null; then
    echo -e "${RED}❌ Alguns processos ainda estão rodando:${NC}"
    ps aux | grep -E "python.*(v4|worker).py" | grep -v grep
else
    echo -e "${GREEN}✅ Todos os processos foram parados${NC}"
fi

echo -e "${BLUE}🏁 Parada completa${NC}"
