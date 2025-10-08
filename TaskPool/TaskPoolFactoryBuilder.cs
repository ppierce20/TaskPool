namespace TaskPool
{
    public class TaskPoolFactoryBuilder<WORKER, INPUT, OUTPUT>(TaskPoolConfigure? configure = null)
        where WORKER : ITaskWorker<INPUT, OUTPUT>
    {
        private TaskPoolConfigure config = configure ?? new TaskPoolConfigure();

        /// <summary>
        /// Sets the minimum number of worker threads for the task pool configuration.
        /// </summary>
        /// <remarks>Setting a higher minimum thread count can improve responsiveness under load but may
        /// increase resource usage. This method does not start threads immediately; it configures the pool for future
        /// operations.</remarks>
        /// <param name="minThreads">The minimum number of threads to maintain in the pool. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT, OUTPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT, OUTPUT> SetMinThreads(int minThreads)
        {
            if (minThreads <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minThreads), "MinThreads must be greater than zero.");
            }

            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(minThreads, config.MaxThreads, config.IdleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the maximum number of worker threads that the task pool can use.
        /// </summary>
        /// <remarks>Setting a higher value for <paramref name="maxThreads"/> may improve concurrency but
        /// can increase resource usage. The actual number of threads used may be limited by system resources.</remarks>
        /// <param name="maxThreads">The maximum number of threads allowed in the pool. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT, OUTPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT, OUTPUT> SetMaxThreads(int maxThreads)
        {
            if (maxThreads <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxThreads), "MaxThreads must be greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, maxThreads, config.IdleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the idle timeout, in seconds, for worker threads in the task pool.
        /// </summary>
        /// <remarks>Setting a lower idle timeout can help release resources more quickly when threads are
        /// not needed, while a higher value may improve performance in scenarios with frequent task bursts.</remarks>
        /// <param name="idleTimeoutSeconds">The number of seconds a worker thread can remain idle before being terminated. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT, OUTPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT, OUTPUT> SetIdleTimeout(int idleTimeoutSeconds)
        {
            if (idleTimeoutSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(idleTimeoutSeconds), "IdleTimeoutSeconds must be  greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, config.MaxThreads, idleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the hold time, in milliseconds, for worker threads before they are released when idle.
        /// </summary>
        /// <remarks>Increasing the hold time may reduce thread churn in scenarios with frequent
        /// short-lived tasks, but can increase resource usage if threads remain idle for longer periods.</remarks>
        /// <param name="holdMiliSeconds">The number of milliseconds that a worker thread should remain held before being released. Must be
        /// greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT, OUTPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT, OUTPUT> SetHoldMiliSeconds(int holdMiliSeconds)
        {
            if (holdMiliSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(holdMiliSeconds), "HoldMiliSeconds must be greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, config.MaxThreads, config.IdleTimeoutSeconds, holdMiliSeconds);
            return this;
        }

        /// <summary>
        /// Creates a new instance of a task pool factory using the specified configuration.
        /// </summary>
        /// <param name="configure">The configuration settings to apply to the task pool factory. Cannot be null.</param>
        /// <returns>A new instance of <see cref="TaskPoolFactory{WORKER, INPUT, OUTPUT}"/> initialized with the provided
        /// configuration.</returns>
        public TaskPoolFactory<WORKER, INPUT, OUTPUT> Create()
        {
            config ??= new TaskPoolConfigure();

            return new TaskPoolFactory<WORKER, INPUT, OUTPUT>(config);
        }
    }

    public class TaskPoolFactoryBuilder<WORKER, INPUT>(TaskPoolConfigure? configure = null)
        where WORKER : ITaskWorker<INPUT>
    {
        private TaskPoolConfigure config = configure ?? new TaskPoolConfigure();

        /// <summary>
        /// Sets the minimum number of worker threads for the task pool configuration.
        /// </summary>
        /// <remarks>Setting a higher minimum thread count can improve responsiveness under load but may
        /// increase resource usage. This method does not start threads immediately; it configures the pool for future
        /// operations.</remarks>
        /// <param name="minThreads">The minimum number of threads to maintain in the pool. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT> SetMinThreads(int minThreads)
        {
            if (minThreads <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(minThreads), "MinThreads must be greater than zero.");
            }

            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(minThreads, config.MaxThreads, config.IdleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the maximum number of worker threads that the task pool can use.
        /// </summary>
        /// <remarks>Setting a higher value for <paramref name="maxThreads"/> may improve concurrency but
        /// can increase resource usage. The actual number of threads used may be limited by system resources.</remarks>
        /// <param name="maxThreads">The maximum number of threads allowed in the pool. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT> SetMaxThreads(int maxThreads)
        {
            if (maxThreads <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(maxThreads), "MaxThreads must be greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, maxThreads, config.IdleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the idle timeout, in seconds, for worker threads in the task pool.
        /// </summary>
        /// <remarks>Setting a lower idle timeout can help release resources more quickly when threads are
        /// not needed, while a higher value may improve performance in scenarios with frequent task bursts.</remarks>
        /// <param name="idleTimeoutSeconds">The number of seconds a worker thread can remain idle before being terminated. Must be greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT> SetIdleTimeout(int idleTimeoutSeconds)
        {
            if (idleTimeoutSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(idleTimeoutSeconds), "IdleTimeoutSeconds must be  greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, config.MaxThreads, idleTimeoutSeconds, config.HoldMiliSeconds);
            return this;
        }

        /// <summary>
        /// Sets the hold time, in milliseconds, for worker threads before they are released when idle.
        /// </summary>
        /// <remarks>Increasing the hold time may reduce thread churn in scenarios with frequent
        /// short-lived tasks, but can increase resource usage if threads remain idle for longer periods.</remarks>
        /// <param name="holdMiliSeconds">The number of milliseconds that a worker thread should remain held before being released. Must be
        /// greater than zero.</param>
        /// <returns>The current instance of <see cref="TaskPoolFactoryBuilder{WORKER, INPUT}"/> to allow method
        /// chaining.</returns>
        public TaskPoolFactoryBuilder<WORKER, INPUT> SetHoldMiliSeconds(int holdMiliSeconds)
        {
            if (holdMiliSeconds <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(holdMiliSeconds), "HoldMiliSeconds must be greater than zero.");
            }
            config ??= new TaskPoolConfigure();
            config = new TaskPoolConfigure(config.MinThreads, config.MaxThreads, config.IdleTimeoutSeconds, holdMiliSeconds);
            return this;
        }

        /// <summary>
        /// Creates a new instance of a task pool factory using the specified configuration.
        /// </summary>
        /// <param name="configure">The configuration settings to apply to the task pool factory. Cannot be null.</param>
        /// <returns>A new instance of <see cref="TaskPoolFactory{WORKER, INPUT}"/> initialized with the provided
        /// configuration.</returns>
        public TaskPoolFactory<WORKER, INPUT> Create()
        {
            config ??= new TaskPoolConfigure();

            return new TaskPoolFactory<WORKER, INPUT>(config);
        }
    }
}
