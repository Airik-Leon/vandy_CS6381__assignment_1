const zmq = require("zeromq");
const uuid = require("uuid");
const portFinder = require("portfinder");
const os = require("os");
const dns = require("dns");
const { promisify } = require("util");

function Subscription(socket, topic, callback) {
  return {
    run() {
      socket.subscribe(topic).on("message", (topicBuffer, messageBuffer) => {
        const message = JSON.parse(new String(messageBuffer));
        const topic = String(topicBuffer);
        callback(topic, message);
      });
    },
  };
}

function Publisher({ publisherSocket, mode }) {
  const id = uuid.v4();

  return {
    mode,
    publish(topic, data) {
      publisherSocket.send([
        topic,
        JSON.stringify({
          message_id: uuid.v4(),
          sender_id: id,
          sent_at: Date.now(),
          data,
        }),
      ]);
    },
  };
}

function PubSubContext({
  subscriberPort,
  publisherPort,
  enableDecentralizedPublisher = false,
  enableDecentralizedSubscriber = false,
  topics = [],
}) {
  return new Promise(async (resolve) => {
    const publisherSocket = zmq.socket("pub");
    let publisher;
    publisherSocket.connect(publisherPort);
    const subscriptions = [];
    const subscriptionHandlers = {};

    const decentralizedPublisherSocket = zmq.socket("pub");
    let ip;

    const instance = {
      subscriberPort,
      publisherPort,
      publisherSocket,
      buildPublisher() {
        if (!publisher) {
          publisher = Publisher({
            publisherSocket: publisherSocket,
            mode: "broker",
          });
        }

        const publishers = [
          publisher,
          ...(enableDecentralizedPublisher
            ? [
                Publisher({
                  publisherSocket: decentralizedPublisherSocket,
                  mode: "local",
                }),
              ]
            : []),
        ];

        if (enableDecentralizedPublisher)
          publisher.publish("register-publisher", { hostname: ip, topics });

        return {
          publish(topic, data) {
            publishers.forEach((publisher) => {
              publisher.publish(topic, data);
            });
          },
        };
      },
      buildSubscription(topic, callback) {
        const subscriberSocket = zmq.socket("sub");
        subscriberSocket.connect(subscriberPort);
        subscriptions.push(Subscription(subscriberSocket, topic, callback));

        if (subscriptionHandlers[topic]) {
          subscriptionHandlers[topic].push(callback);
        } else {
          subscriptionHandlers[topic] = [callback];
        }

        return subscriptions;
      },
    };

    if (enableDecentralizedPublisher) {
      const port = await promisify(portFinder.getPort)();
      const { address } = await promisify(dns.lookup)(os.hostname());
      ip = `tcp://${address}:${port}`;
      decentralizedPublisherSocket.bindSync(ip);
    }

    if (enableDecentralizedSubscriber) {
      instance
        .buildSubscription("register-publisher", (_, message) => {
          const relevantTopics = new Set(topics);

          message.data.topics
            .filter((topic) => relevantTopics.has(topic))
            .map((topic) => {
              const socket = zmq.socket("sub");
              socket.connect(message.data.hostname);
              const [handler] = subscriptionHandlers[topic];
              const subscription = Subscription(socket, topic, handler);

              subscriptions.push(subscription);
              return subscription;
            })
            .forEach((sub) => sub.run());
        })
        .forEach((sub) => sub.run());
    }

    resolve(instance);
  });
}

module.exports = {
  PubSubContext,
};
