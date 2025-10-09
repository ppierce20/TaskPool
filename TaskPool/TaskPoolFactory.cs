using System.Collections.ObjectModel;

namespace TaskPool
{
    public sealed class TaskPoolFactory<WORKER, INPUT, OUTPUT> : TaskPoolFactory<WORKER>, ITaskPoolFactory<WORKER, INPUT, OUTPUT>
        where WORKER : ITaskWorker<INPUT, OUTPUT>
    {
        private readonly Collection<(INPUT, OUTPUT)> Results = [];

        public override int GetWorkQueueCount() => WorkQueue.Count;

        private readonly Collection<INPUT> WorkQueue = [];

        public TaskPoolFactory(ITaskPoolConfigure config) : base(config)
        {
        }

        public IEnumerable<(INPUT, OUTPUT)> GetResults()
        {
            lock (Results)
            {
                var results = Results.ToList();
                Results.Clear();
                return results;
            }
        }

        public void AddWork(INPUT data)
        {
            WorkQueue.Add(data);
        }

        public override void AssignWork()
        {
            if (GetWorkQueueCount() == 0) return;

            var workToAssign = Math.Min(GetIdleWorkerCount(), GetWorkQueueCount());

            // get free workers
            var freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();
            for (int i = 0; i < workToAssign; i++)
            {
                var worker = freeWorkers[i].Value;
                var workItem = WorkQueue[0];
                WorkQueue.RemoveAt(0);
                Task.Run(() =>
                {
                    var result = worker.StartJob(workItem);
                    lock (Results)
                    {
                        Results.Add((workItem, result));
                    }
                });
            }
        }
    }

    public sealed class TaskPoolFactory<WORKER, INPUT> : TaskPoolFactory<WORKER>, ITaskPoolFactory<WORKER, INPUT>
        where WORKER : ITaskWorker<INPUT>
    {
        public override int GetWorkQueueCount() => WorkQueue.Count;

        private readonly Collection<INPUT> WorkQueue = [];

        public TaskPoolFactory(ITaskPoolConfigure config) : base(config)
        {
        }

        public void AddWork(INPUT data)
        {
            WorkQueue.Add(data);
        }

        public override void AssignWork()
        {
            if (GetWorkQueueCount() == 0) return;

            var workToAssign = Math.Min(GetIdleWorkerCount(), GetWorkQueueCount());

            // get free workers
            var freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();
            for (int i = 0; i < workToAssign; i++)
            {
                var worker = freeWorkers[i].Value;
                var workItem = WorkQueue[0];
                WorkQueue.RemoveAt(0);
                Task.Run(() =>
                {
                    worker.StartJob(workItem);
                });
            }
        }
    }

    public abstract class TaskPoolFactory<WORKER> : ITaskPoolFactory<WORKER>, IDisposable
        where WORKER : ITaskWorker
    {
        protected readonly ITaskPoolConfigure _config;
        protected bool ShutdownInitiated;

        protected readonly Dictionary<Guid, WORKER> _TaskWorkers = [];

        public virtual int GetCurrentWorkerCount() => _TaskWorkers.Count;

        public virtual int GetIdleWorkerCount() => _TaskWorkers.Count(w => !w.Value.Working);

        public abstract int GetWorkQueueCount();

        public abstract void AssignWork();

        public TaskPoolFactory(ITaskPoolConfigure config)
        {
            _config = config;
            if (_config.MinThreads > _config.MaxThreads)
            {
                throw new ArgumentException("MinThreads cannot be greater than MaxThreads");
            }
            ShutdownInitiated = false;

            RunAsync();
        }

        public void Dispose()
        {
            if (!ShutdownInitiated)
            {
                ShutdownWorkers();
            }
        }

        public (Guid, WORKER) AddWorker()
        {
            if (_TaskWorkers.Count >= _config.MaxThreads)
            {
                throw new Exception("Max threads reached");
            }

            var guid = Guid.NewGuid();
            var worker = (WORKER)Activator.CreateInstance(typeof(WORKER), true)!;
            lock (_TaskWorkers)
            {
                _TaskWorkers.Add(guid, worker);
            }
            return (guid, worker);
        }

        public ReadOnlyCollection<WORKER> GetWorkers()
        {
            return new ReadOnlyCollection<WORKER>(_TaskWorkers.Select(d => d.Value).ToList());
        }

        public bool RemoveWorker(WORKER worker)
        {
            lock (_TaskWorkers)
            {
                if (_TaskWorkers.Values.Contains(worker))
                {
                    var kvp = _TaskWorkers.First(d => d.Value.Equals(worker));
                    return RemoveWorker(kvp.Key);
                }
                else
                {
                    return false;
                }
            }
        }

        public virtual bool RemoveWorker(Guid key)
        {
            if (_TaskWorkers.ContainsKey(key))
            {
                lock (_TaskWorkers)
                    _TaskWorkers.Remove(key);
                return true;
            }
            else
            {
                return false;
            }
        }

        public virtual bool ShutdownWorkers()
        {
            ShutdownInitiated = true;
            while (_TaskWorkers.Any())
            {
                _TaskWorkers.Where(w => !w.Value.Working).ToList().ForEach(w => RemoveWorker(w.Key));
            }

            return _TaskWorkers.Count == 0;
        }

        public virtual async Task ScaleUpIfNeeded()
        {
            if (GetCurrentWorkerCount() >= _config.MaxThreads) return;

            if (GetWorkQueueCount() == 0) return;

            if (GetIdleWorkerCount() >= GetWorkQueueCount()) return;

            // if we are here we are not at max threads, we have work to do and we don't have enough idle workers to do it all
            var workersToCreate = Math.Min(_config.MaxThreads - GetCurrentWorkerCount(), GetWorkQueueCount() - GetIdleWorkerCount());

            await Task.Run(() =>
            {
                Parallel.For(0, workersToCreate,
                    new ParallelOptions { MaxDegreeOfParallelism = workersToCreate },
                    (i) =>
                    {
                        AddWorker();
                    });
            });
        }

        public virtual async Task RecycleAsync()
        {
            var workersTimedOut = _TaskWorkers.Where(w => !w.Value.Working && (DateTime.UtcNow.Subtract(w.Value.StoppedTime ?? DateTime.UtcNow).Seconds >= _config.IdleTimeoutSeconds)).ToList();
            foreach (var worker in workersTimedOut)
            {
                RemoveWorker(worker.Key);
            }
        }

        public virtual async Task ScaleAsync(int amount)
        {
            if (amount > _config.MaxThreads || amount < _config.MinThreads)
            {
                throw new ArgumentOutOfRangeException($"Amount must be between {_config.MinThreads} and {_config.MaxThreads}");
            }

            if (GetCurrentWorkerCount() == amount)
            {
                return;
            }

            var workersToCreate = _config.MinThreads - _TaskWorkers.Count;

            await Task.Run(() =>
            {
                Parallel.For(0, workersToCreate,
                    new ParallelOptions { MaxDegreeOfParallelism = workersToCreate },
                    (i) =>
                    {
                        AddWorker();
                    });
            });
        }

        public virtual async Task RunAsync()
        {
            while (!ShutdownInitiated)
            {
                if (GetCurrentWorkerCount() < _config.MinThreads)
                    await ScaleAsync(_config.MinThreads - GetCurrentWorkerCount());

                if (GetWorkQueueCount() == 0)
                {
                    await Task.Delay(_config.HoldMiliSeconds);
                    continue;
                }

                await ScaleUpIfNeeded();

                AssignWork();

                await RecycleAsync();

                await Task.Delay(_config.HoldMiliSeconds);
            }
        }
    }
}
