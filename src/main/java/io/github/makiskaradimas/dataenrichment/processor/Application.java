
package io.github.makiskaradimas.dataenrichment.processor;

import io.github.makiskaradimas.dataenrichment.config.TopicsConfig;
import io.github.makiskaradimas.dataenrichment.post.EnrichedPost;
import io.github.makiskaradimas.dataenrichment.post.Post;
import io.github.makiskaradimas.dataenrichment.post.PostSubscriptionListJoin;
import io.github.makiskaradimas.dataenrichment.post.PostWithSubscription;
import io.github.makiskaradimas.dataenrichment.serdes.JsonPOJODeserializer;
import io.github.makiskaradimas.dataenrichment.serdes.JsonPOJOSerializer;
import io.github.makiskaradimas.dataenrichment.serdes.PostDeserializer;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriberValue;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionStatus;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionValue;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionValueList;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class Application {

    private static final Logger log = Logger.getLogger(Application.class.getName());

    public Application() {
    }

    public static void main(String[] args) throws Exception {

        Properties props = new Application().getProperties();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Bytes().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // exactly-once processing
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // prevent marking the coordinator dead
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "3600000");

        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class);

        StreamsBuilder builder = new StreamsBuilder();

        HashMap serdeProps = new HashMap();

        final Serializer<SubscriptionValue> subscriptionValueSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", SubscriptionValue.class);
        subscriptionValueSerializer.configure(serdeProps, false);

        final Deserializer<SubscriptionValue> subscriptionValueDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", SubscriptionValue.class);
        subscriptionValueDeserializer.configure(serdeProps, false);

        final Serde<SubscriptionValue> subscriptionValueSerde = Serdes.serdeFrom(subscriptionValueSerializer, subscriptionValueDeserializer);

        final Serializer<SubscriberValue> subscriberValueSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", SubscriberValue.class);
        subscriberValueSerializer.configure(serdeProps, false);

        final Deserializer<SubscriberValue> subscriberValueDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", SubscriberValue.class);
        subscriberValueDeserializer.configure(serdeProps, false);

        final Serde<SubscriberValue> subscriberValueSerde = Serdes.serdeFrom(subscriberValueSerializer, subscriberValueDeserializer);

        final Serializer<Post> postSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", Post.class);
        postSerializer.configure(serdeProps, false);

        final Deserializer<Post> postDeserializer = new PostDeserializer<>();
        serdeProps.put("DBObjectClass", Post.class);
        postDeserializer.configure(serdeProps, false);

        final Serde<Post> postSerde = Serdes.serdeFrom(postSerializer, postDeserializer);

        final Serializer<PostWithSubscription> postSubscriptionSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", PostWithSubscription.class);
        postSubscriptionSerializer.configure(serdeProps, false);

        final Deserializer<PostWithSubscription> postSubscriptionDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("DBObjectClass", PostWithSubscription.class);
        postSubscriptionDeserializer.configure(serdeProps, false);

        final Serde<PostWithSubscription> postSubscriptionSerde = Serdes.serdeFrom(postSubscriptionSerializer, postSubscriptionDeserializer);

        final Serializer<EnrichedPost> postSubscriberSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", EnrichedPost.class);
        postSubscriberSerializer.configure(serdeProps, false);

        final Deserializer<EnrichedPost> postSubscriberDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("DBObjectClass", EnrichedPost.class);
        postSubscriberDeserializer.configure(serdeProps, false);

        final Serde<EnrichedPost> postSubscriberSerde = Serdes.serdeFrom(postSubscriberSerializer, postSubscriberDeserializer);

        final Serializer<SubscriptionValueList> subscriptionValueListSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", SubscriptionValueList.class);
        subscriptionValueListSerializer.configure(serdeProps, false);

        final Deserializer<SubscriptionValueList> subscriptionValueListDeserializer = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", SubscriptionValueList.class);
        subscriptionValueListDeserializer.configure(serdeProps, false);

        final Serde<SubscriptionValueList> subscriptionValueListSerde = Serdes.serdeFrom(subscriptionValueListSerializer, subscriptionValueListDeserializer);

        final KStream<Post, Post> posts = builder.stream(TopicsConfig.POSTS_TOPIC_NAME, Consumed.with(postSerde, postSerde));
        final KStream<Bytes, SubscriptionValue> subscriptions = builder.stream(TopicsConfig.SUBSCRIPTIONS_INPUT_TOPIC_NAME, Consumed.with(Serdes.Bytes(), subscriptionValueSerde));
        final KTable<Bytes, SubscriberValue> subscriberTable = builder.table(TopicsConfig.SUBSCRIBERS_INPUT_TOPIC_NAME, Consumed.with(Serdes.Bytes(), subscriberValueSerde), Materialized.as("subscriber-store"));

        KTable<Bytes, SubscriptionValueList> subscriptionTable = subscriptions
                .map((key, value) -> new KeyValue<>(key, new String(subscriptionValueSerializer.serialize(null, value))))
                .groupByKey()
                .aggregate(() -> {
                    SubscriptionValueList subscriptionValueList = new SubscriptionValueList();
                    subscriptionValueList.setSubscriptionValues(new ArrayList<>());
                    return subscriptionValueList;
                }, (subscriptionKey, subscriptionStr, subscriptionValueList) -> {
                    List<SubscriptionValue> values = subscriptionValueList.getSubscriptionValues();
                    SubscriptionValue subscriptionValue = subscriptionValueDeserializer.deserialize(null, subscriptionStr.getBytes());
                    if (subscriptionValue.getStatus() == SubscriptionStatus.UNSUBSCRIBED) {
                        for (SubscriptionValue value : values) {
                            if (value.getSubscriberId().equals(subscriptionValue.getSubscriberId())
                                    && value.getCategory().equals(subscriptionValue.getCategory())) {
                                values.remove(value);
                                break;
                            }
                        }
                    } else {
                        boolean found = false;
                        for (SubscriptionValue value : values) {
                            if (value.getSubscriberId().equals(subscriptionValue.getSubscriberId())
                                    && value.getCategory().equals(subscriptionValue.getCategory())) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            values.add(subscriptionValue);
                        }
                    }
                    return subscriptionValueList;
                }, Materialized.as("subscriptions-store").with(null, subscriptionValueListSerde));

        KStream<Bytes, EnrichedPost> enrichedPosts = posts
                .map((key, value) -> new KeyValue<>(new Bytes(value.getId().getBytes()), new String(postSerializer.serialize(null, value))))
                .groupByKey()
                .aggregate(Post::new, (key, value, newPost) -> {
                    Post post = postDeserializer.deserialize(null, value.getBytes());
                    return newPost.update(post);
                }, Materialized.as("post-store").with(null, postSerde))
                .toStream()
                .filter((key, post) -> (post.getId() != null && post.getStatus() != null && post.getStatus().equals("Published")))
                .map((key, post) -> new KeyValue<>(Bytes.wrap(post.getCategory().getBytes()), post))
                .join(subscriptionTable, (post, subscriptionValueList) -> {
                    PostSubscriptionListJoin postSubscriptionListJoin;

                    if (subscriptionValueList != null) {
                        postSubscriptionListJoin = new PostSubscriptionListJoin(post, subscriptionValueList);
                    } else {
                        postSubscriptionListJoin = new PostSubscriptionListJoin();
                    }

                    return postSubscriptionListJoin;
                }, Joined.with(Serdes.Bytes(), postSerde, null))
                .flatMapValues(postSubscriptionListJoin -> {
                    List<PostWithSubscription> entries = new ArrayList<>();
                    for (SubscriptionValue subscriptionValue : postSubscriptionListJoin.getSubscriptionValues()) {
                        PostWithSubscription postWithSubscription = new PostWithSubscription(postSubscriptionListJoin.getPost(), subscriptionValue);
                        entries.add(postWithSubscription);
                    }
                    return entries;
                })
                .map((key, value) -> new KeyValue<>(Bytes.wrap(value.getSubscriberId().getBytes()), value))
                .join(subscriberTable, (postPublisherJoin, subscriberValue) -> {
                    EnrichedPost enrichedPost;

                    if (subscriberValue != null) {
                        enrichedPost = new EnrichedPost(postPublisherJoin, subscriberValue);
                    } else {
                        enrichedPost = new EnrichedPost();
                    }

                    return enrichedPost;
                }, Joined.with(Serdes.Bytes(), postSubscriptionSerde, null));

        enrichedPosts.to(TopicsConfig.OUTPUT_TOPIC_NAME, Produced.with(Serdes.Bytes(), postSubscriberSerde,
                (key, value, numPartitions) -> value.getPartition() % numPartitions));
        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));

        // In case of uncaught exception in StreamThread (usually TopologyBuilderException) restart without calling the shutdown hook
        Thread.getAllStackTraces().keySet().forEach((t) -> {
            if (t.getName().startsWith("StreamThread")) {
                t.setUncaughtExceptionHandler((t1, e) -> {
                    log.severe("The stream processor has to restart because of an uncaught exception: " + e.getMessage());
                    Runtime.getRuntime().halt(0);
                });
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    private Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("application.properties"));
        return properties;
    }

}
