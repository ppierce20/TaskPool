namespace TaskPool
{
    public interface ITaskWorker<in INPUT, out OUTPUT> : ITaskWorker
    {
        public OUTPUT StartJob(INPUT data);
    }

    public interface ITaskWorker<in INPUT> : ITaskWorker
    {
        public void StartJob(INPUT data);
    }

    public interface ITaskWorker
    {
        public bool Working { get; }

        public DateTime? StoppedTime { get; }

    }
}
