using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using UnityEngine;
using Confluent.Kafka;

public class main : MonoBehaviour
{
	IConsumer<Ignore, string> c;
	ConsumerConfig conf;
	CancellationTokenSource cts;
	bool logging = false;

	// Start is called before the first frame update
	void Start()
	{
		Debug.Log("main start");
		//ConsumerBuilder<Ignore, string> c = new ConsumerBuilder<Ignore, string>(conf).Build()
		conf = new ConsumerConfig{
			GroupId = "test-consumer-group",
			BootstrapServers = "localhost:9092",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};

		c = new ConsumerBuilder<Ignore, string>(conf).Build();

		c.Subscribe("testTopicName");

		cts = new CancellationTokenSource();
		Debug.Log("main started");
	}

	public void toggleLogging()
	{
		logging = !logging;
	}

	// Update is called once per frame
	void Update()
    {
		if (logging)
		{
			try
			{
				var cr = c.Consume(cts.Token);
				Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
			}
			catch (ConsumeException e)
			{
				Console.WriteLine($"Error occured: {e.Error.Reason}");
			}
			catch (OperationCanceledException)
			{
				Console.WriteLine("cancelled");
				// Ensure the consumer leaves the group cleanly and final offsets are committed.
				c.Close();
			}
		}
		else
		{
			Debug.Log("not logging");
		}
    }
}
