# !/bin/bash

# Criar diretórios
mkdir -p logs "sessions/[user_id]" backups

# Criar arquivos de log
touch logs/bot.log
touch logs/worker.log
touch logs/error.log
touch logs/worker_errors.log

# Criar arquivos de sessões e backups (exemplares)
touch "sessions/[user_id]/[phone]_[user_id].session"
touch backups/database_backup_YYYY-MM-DD.sql
touch backups/sessions_backup_YYYY-MM-DD.tar.gz

# Criar arquivos principais do projeto
touch v4.py
touch async_helper.py
touch tasks.py
touch shared.py
touch worker.py
touch .env
touch .env.example
touch requirements.txt
touch start_bot.sh
touch start_worker.sh
touch start_multiple_workers.sh
touch stop_all.sh
touch restart_system.sh
touch monitor.sh
touch backup.sh
touch deploy.sh
touch README.md
touch CHANGELOG.md
touch LICENSE
touch .gitignore

echo "✅ Estrutura criada com sucesso!"
