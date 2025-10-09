using System.Collections.ObjectModel;

namespace TaskPool
{
    public interface ITaskPoolFactory<WORKER, INPUT, OUTPUT> : ITaskPoolFactory<WORKER>
        where WORKER : ITaskWorker<INPUT, OUTPUT>
    {
        public void AddWork(INPUT data);

        public IEnumerable<(INPUT, OUTPUT)> GetResults();
    }

    public interface ITaskPoolFactory<WORKER, INPUT> : ITaskPoolFactory<WORKER>
        where WORKER : ITaskWorker<INPUT>
    {
        public void AddWork(INPUT data);
    }

    public interface ITaskPoolFactory<WORKER> : ITaskPoolFactory
    {
        public (Guid, WORKER) AddWorker();

        public bool RemoveWorker(WORKER worker);

        public ReadOnlyCollection<WORKER> GetWorkers();
    }

    public interface ITaskPoolFactory
    {
        public bool RemoveWorker(Guid key);

        public bool ShutdownWorkers();

        public Task ScaleAsync(int amount);

        public Task RecycleAsync();

        public int GetWorkQueueCount();

        public int GetIdleWorkerCount();

        public int GetCurrentWorkerCount();

        public Task ScaleUpIfNeeded();

        public void AssignWork();

        public Task RunAsync();
    }
}
