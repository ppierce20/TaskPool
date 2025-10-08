namespace TaskPool
{
    public sealed class TaskPoolConfigure(int minThreads = 1, int maxThreads = 20, int idleTimeoutSeconds = 600, int holdMiliSeconds = 1000) : ITaskPoolConfigure
    {
        public int MinThreads { get; private set; } = minThreads;
        public int MaxThreads { get; private set; } = maxThreads;
        public int IdleTimeoutSeconds { get; private set; } = idleTimeoutSeconds;
        public int HoldMiliSeconds { get; private set; } = holdMiliSeconds;
    }
}
