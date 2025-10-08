using System.Collections.ObjectModel;

namespace TaskPool
{
    public sealed class TaskPoolFactory<WORKER, INPUT, OUTPUT>
        : ITaskPoolFactory<WORKER, INPUT, OUTPUT>, IDisposable
        where WORKER : ITaskWorker<INPUT, OUTPUT>
    {
        private readonly ITaskPoolConfigure _config;
        private readonly Dictionary<Guid, WORKER> _TaskWorkers = [];
        public int WorkerCount => _TaskWorkers.Count;
        private bool ShutdownInitiated;
        public bool WorkQueueEmpty => WorkQueue.Count == 0;

        private readonly Collection<(INPUT, OUTPUT)> Results = [];
        private readonly Collection<INPUT> WorkQueue = [];

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

        public void AddWork(INPUT data)
        {
            WorkQueue.Add(data);
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

        public bool RemoveWorker(WORKER worker)
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

        public bool RemoveWorker(Guid key)
        {
            if (_TaskWorkers.ContainsKey(key))
            {
                _TaskWorkers.Remove(key);
                return true;
            }
            else
            {
                return false;
            }
        }

        public ReadOnlyCollection<WORKER> GetWorkers()
        {
            return new ReadOnlyCollection<WORKER>(_TaskWorkers.Select(d => d.Value).ToList());
        }

        public bool ShutdownWorkers()
        {
            ShutdownInitiated = true;
            while (_TaskWorkers.Any())
            {
                _TaskWorkers.Where(w => !w.Value.Working).ToList().ForEach(w => RemoveWorker(w.Key));
            }

            return _TaskWorkers.Count == 0;
        }

        public void Dispose()
        {
            if (!ShutdownInitiated)
            {
                ShutdownWorkers();
            }
        }

        public async void RunAsync()
        {
            while (!ShutdownInitiated)
            {
                await LoadToMinAsync();

                if (WorkQueue.Count == 0)
                {
                    await Task.Delay(_config.HoldMiliSeconds);
                    continue;
                }

                // if we are here we have work to do

                // get free workers
                var freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();

                if (freeWorkers.Count < WorkQueue.Count) //we don't have enough workers for all the work
                {
                    // create more workers if possible
                    var workersToCreate = Math.Min(_config.MaxThreads - _TaskWorkers.Count, WorkQueue.Count - freeWorkers.Count);
                    if (workersToCreate > 0)
                    {
                        await Task.Run(() =>
                        {
                            Parallel.For(0, workersToCreate,
                                new ParallelOptions { MaxDegreeOfParallelism = workersToCreate },
                                (i) =>
                                {
                                    AddWorker();
                                });
                        });
                        // refresh free workers list
                        freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();
                    }
                }

                var workToAssign = Math.Min(freeWorkers.Count, WorkQueue.Count);

                for (int i = 0; i < workToAssign; i++)
                {
                    var worker = freeWorkers[i].Value;
                    var workItem = WorkQueue[0];
                    WorkQueue.RemoveAt(0);
                    _ = Task.Run(() =>
                    {
                        var result = worker.StartJob(workItem);
                        lock (Results)
                        {
                            Results.Add((workItem, result));
                        }
                    });
                }

                var workersTimedOut = _TaskWorkers.Where(w => !w.Value.Working && (DateTime.UtcNow.Subtract(w.Value.StoppedTime ?? DateTime.UtcNow).Seconds >= _config.IdleTimeoutSeconds)).ToList();
                foreach (var worker in workersTimedOut)
                {
                    RemoveWorker(worker.Key);
                }

                await Task.Delay(_config.HoldMiliSeconds);
            }
        }

        public async Task LoadToMinAsync()
        {
            if (_TaskWorkers.Count >= _config.MinThreads) return;

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
    }

    public sealed class TaskPoolFactory<WORKER, INPUT>
        : ITaskPoolFactory<WORKER, INPUT>, IDisposable
        where WORKER : ITaskWorker<INPUT>
    {
        private readonly ITaskPoolConfigure _config;
        private readonly Dictionary<Guid, WORKER> _TaskWorkers = [];

        public bool WorkQueueEmpty => WorkQueue.Count == 0;
        public int WorkerCount => _TaskWorkers.Count;
        private bool ShutdownInitiated;

        private readonly Collection<INPUT> WorkQueue = [];

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

        public void AddWork(INPUT data)
        {
            WorkQueue.Add(data);
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

        public bool RemoveWorker(WORKER worker)
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

        public bool RemoveWorker(Guid key)
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

        public ReadOnlyCollection<WORKER> GetWorkers()
        {
            return new ReadOnlyCollection<WORKER>(_TaskWorkers.Select(d => d.Value).ToList());
        }

        public bool ShutdownWorkers()
        {
            ShutdownInitiated = true;
            while (_TaskWorkers.Any())
            {
                _TaskWorkers.Where(w => !w.Value.Working).ToList().ForEach(w => RemoveWorker(w.Key));
            }

            return _TaskWorkers.Count == 0;
        }

        public void Dispose()
        {
            if (!ShutdownInitiated)
            {
                ShutdownWorkers();
            }
        }

        public async void RunAsync()
        {
            while (!ShutdownInitiated)
            {
                await LoadToMinAsync();

                if (WorkQueue.Count == 0)
                {
                    await Task.Delay(_config.HoldMiliSeconds);
                    continue;
                }

                // if we are here we have work to do

                // get free workers
                var freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();

                if (freeWorkers.Count < WorkQueue.Count) //we don't have enough workers for all the work
                {
                    // create more workers if possible
                    var workersToCreate = Math.Min(_config.MaxThreads - _TaskWorkers.Count, WorkQueue.Count - freeWorkers.Count);
                    if (workersToCreate > 0)
                    {
                        await Task.Run(() =>
                        {
                            Parallel.For(0, workersToCreate,
                                new ParallelOptions { MaxDegreeOfParallelism = workersToCreate },
                                (i) =>
                                {
                                    AddWorker();
                                });
                        });
                        // refresh free workers list
                        freeWorkers = _TaskWorkers.Where(w => !w.Value.Working).ToList();
                    }
                }

                var workToAssign = Math.Min(freeWorkers.Count, WorkQueue.Count);

                for (int i = 0; i < workToAssign; i++)
                {
                    var worker = freeWorkers[i].Value;
                    var workItem = WorkQueue[0];
                    WorkQueue.RemoveAt(0);
                    _ = Task.Run(() =>
                    {
                        worker.StartJob(workItem);
                    });
                }

                var workersTimedOut = _TaskWorkers.Where(w => !w.Value.Working && (DateTime.UtcNow.Subtract(w.Value.StoppedTime ?? DateTime.UtcNow).Seconds >= _config.IdleTimeoutSeconds)).ToList();
                foreach (var worker in workersTimedOut)
                {
                    RemoveWorker(worker.Key);
                }

                await Task.Delay(_config.HoldMiliSeconds);
            }
        }

        public async Task LoadToMinAsync()
        {
            if (_TaskWorkers.Count >= _config.MinThreads) return;

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
    }
}
