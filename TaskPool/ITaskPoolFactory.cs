using System.Collections.ObjectModel;

namespace TaskPool
{
    public interface ITaskPoolFactory<WORKER, INPUT, OUTPUT> : ITaskPoolFactory
        where WORKER : ITaskWorker<INPUT, OUTPUT>
    {
        public void AddWork(INPUT data);

        public IEnumerable<(INPUT, OUTPUT)> GetResults();

        public (Guid, WORKER) AddWorker();

        public bool RemoveWorker(WORKER worker);

        public ReadOnlyCollection<WORKER> GetWorkers();
    }

    public interface ITaskPoolFactory<WORKER, INPUT> : ITaskPoolFactory
        where WORKER : ITaskWorker<INPUT>
    {
        public void AddWork(INPUT data);

        public (Guid, WORKER) AddWorker();

        public bool RemoveWorker(WORKER worker);

        public ReadOnlyCollection<WORKER> GetWorkers();
    }

    public interface ITaskPoolFactory
    {
        public int WorkerCount { get; }

        public bool WorkQueueEmpty { get; }

        public bool RemoveWorker(Guid key);

        public bool ShutdownWorkers();

        public void RunAsync();

        public Task LoadToMinAsync();

    }
}
