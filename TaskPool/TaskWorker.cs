namespace TaskPool
{
    public abstract class TaskWorker<INPUT, OUTPUT> : TaskWorker, ITaskWorker<INPUT, OUTPUT>
    {
        public OUTPUT? Result { get; protected set; }

        public virtual OUTPUT StartJob(INPUT data)
        {
            Working = true;
            Result = default;
            StoppedTime = null;

            Result = this.DoWork(data);

            StoppedTime = DateTime.UtcNow;
            Working = false;
            return Result;
        }

        protected abstract OUTPUT DoWork(INPUT data);
    }

    public abstract class TaskWorker<INPUT> : TaskWorker, ITaskWorker<INPUT>
    {
        public virtual void StartJob(INPUT data)
        {
            Working = true;
            StoppedTime = null;

            DoWork(data);

            StoppedTime = DateTime.UtcNow;
            Working = false;
        }

        protected abstract void DoWork(INPUT data);
    }

    public class TaskWorker : ITaskWorker
    {
        public bool Working { get; protected set; }

        public DateTime? StoppedTime { get; protected set; }
    }
}
