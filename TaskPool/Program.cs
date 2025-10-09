
namespace TaskPool
{
    internal class Program
    {
        static void Main(string[] args)
        {
            // Example usage of TaskPoolFactory with MyWorker and int input with no output - builder pattern
            var taskPoolFactory = new TaskPoolFactoryBuilder<MyWorker, int>()
                .SetMinThreads(2)
                .SetMaxThreads(4)
                .Create();

            taskPoolFactory.AddWork(10);
            Console.WriteLine($"taskPoolFactory - Adding Work (10)");
            taskPoolFactory.AddWork(20);
            Console.WriteLine($"taskPoolFactory - Adding Work (20)");
            taskPoolFactory.AddWork(30);
            Console.WriteLine($"taskPoolFactory - Adding Work (30)");
            taskPoolFactory.AddWork(40);
            Console.WriteLine($"taskPoolFactory - Adding Work (40)");

            while (taskPoolFactory.GetWorkers().Any(worker => worker.Working) || !(taskPoolFactory.GetWorkQueueCount() == 0))
            {
                Console.WriteLine($"taskPoolFactory - Working...");
                Thread.Sleep(500);
            }

            Console.WriteLine($"taskPoolFactory - Finished");
            taskPoolFactory.ShutdownWorkers();

            // Example usage of TaskPoolFactory with URLWorker, string input (URL) with httpResponseMessage as the output - builder pattern
            var URLPoolFactory = new TaskPoolFactoryBuilder<URLWorker, string, HttpResponseMessage>()
                .SetMinThreads(2)
                .SetMaxThreads(3)
                .Create();

            URLPoolFactory.AddWork("https://www.example.com/");
            Console.WriteLine($"URLPoolFactory - Adding Work (https://www.example.com/)");
            URLPoolFactory.AddWork("https://www.example.org/");
            Console.WriteLine($"URLPoolFactory - Adding Work (https://www.example.org/)");
            URLPoolFactory.AddWork("https://www.example.net/");
            Console.WriteLine($"URLPoolFactory - Adding Work (https://www.example.net/)");
            URLPoolFactory.AddWork("https://www.example.edu/");
            Console.WriteLine($"URLPoolFactory - Adding Work (https://www.example.edu/)");

            // In this example we have 4 URLs to process and a max of 3 workers so one URL will wait until a worker is free
            while (URLPoolFactory.GetWorkers().Any(worker => worker.Working) || !(URLPoolFactory.GetWorkQueueCount() == 0))
            {
                Console.WriteLine($"URLPoolFactory - Working...");
                Thread.Sleep(500);
            }

            Console.WriteLine($"URLPoolFactory - Finished");
            var results = URLPoolFactory.GetResults();

            foreach (var (url, response) in results)
            {
                Console.WriteLine($"URL: {url}, Status Code: {response.StatusCode}");
            }

            URLPoolFactory.ShutdownWorkers();
        }

        // Example worker with input only. You only need to implement DoWork method and the rest is handled by TaskWorker base class and TaskPoolFactory
        // When using TaskPoolFactory work needs to be added with AddWork method. Tasks will scale up and down as needed and work will be done in the background
        private class MyWorker : TaskWorker<int>
        {
            protected override void DoWork(int data)
            {
                Task.Delay(data * 100).Wait(); // Simulate work by delaying
            }
        }

        // Example worker with input and output. You only need to implement DoWork method and the rest is handled by TaskWorker base class and TaskPoolFactory
        // When using TaskPoolFactory work needs to be added with AddWork method and results can be retrieved with GetResults method
        // Tasks will scale up and down as needed and work will be done in the background
        private class URLWorker : TaskWorker<string, HttpResponseMessage>
        {
            private readonly HttpClient _client = new();

            protected override HttpResponseMessage DoWork(string url)
            {
                return(_client.GetAsync(url).Result);
            }
        }
    }
}
