package my.learn.kafka.demo;

public class DemoApp {

    private final String[] words = new String[] {
            "There",
            "are",
            "many",
            "variations",
            "of",
            "passages",
            "of",
            "Lorem",
            "Ipsum",
            "available,",
            "but",
            "the",
            "majority",
            "have",
            "suffered",
            "alteration",
            "in",
            "some",
            "form,",
            "by",
            "injected",
            "humour,",
            "or",
            "randomised",
            "words",
            "which",
            "don't",
            "look",
            "even",
            "slightly",
            "believable.",
            "If",
            "you",
            "are",
            "going",
            "to",
            "use",
            "a",
            "passage",
            "of",
            "Lorem",
            "Ipsum,",
            "you",
            "need",
            "to",
            "be",
            "sure",
            "there",
            "isn't",
            "anything",
            "embarrassing",
            "hidden",
            "in",
            "the",
            "middle",
            "of",
            "text.",
            "All",
            "the",
            "Lorem",
            "Ipsum",
            "generators",
            "on",
            "the",
            "Internet",
            "tend",
            "to",
            "repeat",
            "predefined",
            "chunks",
            "as",
            "necessary,",
            "making",
            "this",
            "the",
            "first",
            "true",
            "generator",
            "on",
            "the",
            "Internet.",
            "It",
            "uses",
            "a",
            "dictionary",
            "of",
            "over",
            "200"
    };

    public static void run() {
        String topic = "demo-topic";
        String key = "test-key";

        SimpleProducer producer = SimpleProducer.instance();
        producer.sendMessage(topic, key, "value");

        SimpleConsumer consumer = SimpleConsumer.instance();
        consumer.subscribeToTopic(topic);
        consumer.listen();
    }



}
