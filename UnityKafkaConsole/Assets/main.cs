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
		print("main start");
		//ConsumerBuilder<Ignore, string> c = new ConsumerBuilder<Ignore, string>(conf).Build()
		conf = new ConsumerConfig{
			GroupId = "test-consumer-group",
			BootstrapServers = "localhost:9092",
			AutoOffsetReset = AutoOffsetReset.Earliest
		};

		c = new ConsumerBuilder<Ignore, string>(conf).Build();

		c.Subscribe("testTopicName");

		cts = new CancellationTokenSource();
		print("main started");
	}

	public void toggleLogging()
	{
		print("in toggleLogging");
		logging = !logging;
	}

	public void consumeOnce()
	{
		print("in consumeOnce");
		if (logging)
		{
			print("logging");
			try
			{
				print("try");
				var cr = c.Consume(cts.Token);
				print($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
			}
			catch (ConsumeException e)
			{
				print("catch 1");
				print($"Error occured: {e.Error.Reason}");
			}
			catch (OperationCanceledException)
			{
				print("catch 2");
				print("cancelled");
				// Ensure the consumer leaves the group cleanly and final offsets are committed.
				c.Close();
			}


			print("finally");
		}
		else
		{
			print("not logging");
		}
	}

	// Update is called once per frame
	void Update()
	{
		print("in update, logging:" + logging);
		if (logging)
		{
			print("logging");
			try
			{
				print("try");
				var cr = c.Consume(cts.Token);
				print($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
			}
			catch (ConsumeException e)
			{
				print("catch 1");
				print($"Error occured: {e.Error.Reason}");
			}
			catch (OperationCanceledException)
			{
				print("catch 2");
				print("cancelled");
				// Ensure the consumer leaves the group cleanly and final offsets are committed.
				c.Close();
			}


			print("finally");
		}
		else
		{
			print("not logging");
		}
	}

	void print(string msg)
	{
		Debug.Log(msg);
	}
}
