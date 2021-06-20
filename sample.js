const pubsub = require("./common/pubsub");

const main = async () => {
  // SUBSCRIBER Setup
  const context = await pubsub.PubSubContext({
    subscriberPort: "tcp://localhost:5000",
    publisherPort: "tcp://localhost:4000",
    enableDecentralizedSubscriber: true,
    topics: ["topic"],
  });

  let count = 0;
  context
    .buildSubscription("topic", (topic, message) => {
      console.log(count++, topic);
    })
    .forEach((subscription) => {
      subscription.run();
    });

  // PUBLISHER Setup
  const subContext = await pubsub.PubSubContext({
    subscriberPort: "tcp://localhost:5000",
    publisherPort: "tcp://localhost:4000",
    enableDecentralizedPublisher: true,
    topics: ["topic"],
  });

  const publisher = subContext.buildPublisher();

  setInterval(() => {
    publisher.publish("topic", { data: 34 });
  }, 3000);
};

main();
