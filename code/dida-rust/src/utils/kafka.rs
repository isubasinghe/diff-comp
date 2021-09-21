use futures::future::{self, FutureExt};
use futures::stream::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::AsyncRuntime;
use std::future::Future;
use std::pin::Pin;
use tokio;

pub struct AsyncStdRunTime;

impl AsyncRuntime for AsyncStdRunTime {
    type Delay = Pin<Box<dyn Future<Output = ()> + 'static + Send>>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(task);
    }

    fn delay_for(duration: std::time::Duration) -> Self::Delay {
        Box::pin(tokio::time::sleep(duration))
    }
}

pub async fn start_kafka(bootstrap_servers: String, topic: String) {
    let consumer: StreamConsumer<_, AsyncStdRunTime> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("group.id", "rust-rdkafka-consumer")
        .create()
        .expect("failed to create kafka consumer");
    consumer
        .subscribe(&[&topic])
        .expect("failed to subscribe to topic");

    let mut stream = consumer.stream();

    loop {
        let message = stream.next().await;
        match message {
            Some(Ok(msg)) => {}
            Some(Err(e)) => {}
            None => {}
        }
    }
}
