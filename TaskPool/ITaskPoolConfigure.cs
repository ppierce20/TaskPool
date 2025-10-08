namespace TaskPool
{
    public interface ITaskPoolConfigure
    {
        public int MinThreads { get; }
        public int MaxThreads { get; }
        public int IdleTimeoutSeconds { get; }
        public int HoldMiliSeconds { get; }
    }
}
