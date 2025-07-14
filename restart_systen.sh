# Reiniciar completo
bash stop_all.sh
sleep 5
bash start_multiple_workers.sh 3
sleep 10
bash start_bot.sh

# Reiniciar apenas workers
pkill -f "python.*worker.py"
sleep 3
bash start_multiple_workers.sh 3

# Restart individual de worker
kill $(cat pids/worker-1.pid)
WORKER_ID=1 bash start_worker.sh
