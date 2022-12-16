import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import pucrio.br.lac.utils.ConsumerCreator;
import pucrio.br.lac.utils.ShutdownableThread;

import java.util.ArrayList;
import java.util.Map;

public class Main {

    public static void main(String[] args){
        System.out.println("Hello!");

        ArrayList<String> topicNames = new ArrayList<String>();
        topicNames.add("Q12O"); // Sintaxe do output topic = Q + "numero da query" + O. Como estamos lidando com a query 12, fica Q12O o nome do topico.

        runConsumer(topicNames, "BeawarosConsumer");
    }

    /********************* Kafka Methods **********************/

    /**
     * Metodo para executar um consumidor Kafka.
     *
     * @param topicName
     * @param consumerGroupName
     */
    public static void runConsumer(ArrayList<String> topicName, String consumerGroupName)
    {
        System.out.println("ScriptToConsume: runConsumer");

        KafkaConsumer<Integer, String> consumer = ConsumerCreator.createConsumer(topicName,
                "localhost:9092",
                consumerGroupName,
                1,
                "earliest");

        System.out.println("Consumer started on topics = "+ topicName );

        Main.ThreadConsumer scriptConsumer = new Main.ThreadConsumer(consumer, consumerGroupName);
        scriptConsumer.start();
    }

    /**
     * Classe que representa a Thread que o consumidor kafka vai ficar rodando.
     * Ela é necessária se não a execucao do programa principal para por causa do consumer.
     */
    private static class ThreadConsumer extends ShutdownableThread {
        private final KafkaConsumer<Integer, String> _consumer;

        public ThreadConsumer(KafkaConsumer<Integer, String> consumer, String consumerName)
        {
            super(consumerName, false);
            _consumer = consumer;
        }

        @Override
        public void doWork() {
            System.out.println("Waiting for new messages...");

            while (true) {
                final ConsumerRecords<Integer, String> consumerRecords = _consumer.poll(1000);

                for (ConsumerRecord<Integer, String> record : consumerRecords) {
                    System.out.println("------ New data received at ScriptToConsume ------");
                    System.out.println("Record Key " + record.key());
                    System.out.println("ScriptToConsume Consumer: Record value " + record.value());
                    //System.out.println("Record partition " + record.partition());
                    //System.out.println("Record offset " + record.offset());

                    // TODO: Do something here...
                }

                //consumer.commitAsync();

                // We send the commit and carry on, but if the commit fails, the failure and the offsets will be logged.
                _consumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition,
                                                OffsetAndMetadata> offsets, Exception e) {
                        if (e != null)
                        {
                            System.out.println("Commit failed for offsets: "+ offsets+". Exeption = "+ e);
                        }
                    }
                });
            }
        }

        @Override
        public void onShutDown()
        {
            _consumer.close();
        }

    }
}
