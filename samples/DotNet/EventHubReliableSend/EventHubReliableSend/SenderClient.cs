//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace EventHubReliableSend
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.ServiceBus;
    using Microsoft.ServiceBus.Messaging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    class SenderClient
    {
        CancellationToken cancelToken;
        EventHubClient ehClient;
        Task sendTask;
        Random rnd;

        // our emulated parking garage
        private List<dynamic> parking = new List<dynamic>();

        public int totalNumberOfEventsSent = 0;

        public SenderClient(int clientInd, ServiceBusConnectionStringBuilder csb, string ehName, CancellationToken cancelToken)
        {
            this.ClientInd = clientInd;
            this.cancelToken = cancelToken;
            this.rnd = new Random();

            // Create Event Hubs client.
            var mf = MessagingFactory.CreateFromConnectionString(csb.ToString());
            this.ehClient = mf.CreateEventHubClient(ehName);

            // Starts sends
            this.sendTask = StartSendsAsync();
        }

        public Task SendTask { get => sendTask; }

        public int ClientInd { get; }

        async Task StartSendsAsync()
        {
            bool sleepBeforeNextSend = false;

            Console.WriteLine("Client-{0}: Starting to send events.", this.ClientInd);

            while (!this.cancelToken.IsCancellationRequested)
            {
                try
                {
                    // Prepare next batch to send.
                    var newBatch = GenerateNextBatch();

                    // Send batch now.
                    await this.ehClient.SendBatchAsync(newBatch.ToEnumerable());
                    var ret = Interlocked.Add(ref totalNumberOfEventsSent, newBatch.Count);
                }
                catch (Exception ex)
                {
                    if (ex is ServerBusyException)
                    {
                        Console.WriteLine("Client-{0}: Going a little faster than what your namespace TU setting allows. Slowing down now.", this.ClientInd);
                        sleepBeforeNextSend = true; 
                    }
                    else
                    {
                        // Log the rest of the exceptions.
                        Console.WriteLine("Client-{0}: Caught exception while attempting to send: {1}", this.ClientInd, ex.Message);
                    }
                }

                if (sleepBeforeNextSend)
                {
                    await Task.Delay(10000);
                    sleepBeforeNextSend = false;
                }
            }

            Console.WriteLine("Client-{0}: Stopped sending events. Total number of events sent: {1}", this.ClientInd, totalNumberOfEventsSent);
        }

        private string GenerateLicencePlate()
        {
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            var stringChars = new char[8];
            var random = new Random();

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(chars.Length)];
            }

            var finalString = new String(stringChars);
            return finalString;
        }

        EventDataBatch GenerateNextBatch()
        {
            var ehBatch = this.ehClient.CreateBatch();
            for (int i = 0; i < 10; i++)
            {
                // Send random size payloads.
                //var payload = new byte[this.rnd.Next(1024)];
                //this.rnd.NextBytes(payload);

                Random rnd = new Random();
                int op = rnd.Next(0, 2); //generate an OP, either enter or exit

                byte[] body = null;

                if (op == 0) 
                {
                    dynamic e = new JObject();
                    //populate a new car
                    e.Time = DateTime.Now;
                    e.Plate = GenerateLicencePlate();
                    e.Op = op;
                    e.Id = rnd.Next(1, 2000); // get from available plate

                    parking.Add(e);
                    body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(e));
                }
                if (op == 1)
                {
                    rnd = new Random();
                    int idx = rnd.Next(0, parking.Count); //random car from is leaving the garage

                    if (parking.Count > idx)
                    {
                        dynamic e = parking[idx];
                        e.Time = DateTime.Now.AddMinutes(rnd.Next(10, 30));
                        e.Op = op;
                        parking.RemoveAt(idx);
                        body = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(e));
                    }
                }

                if (body != null)
                {
                    var ed = new EventData(body);

                    ed.Properties["clientind"] = this.ClientInd;

                    if (!ehBatch.TryAdd(ed))
                    {
                        break;
                    }
                }

                Thread.Sleep(1000);
            }

            return ehBatch;
        }
    }
}
