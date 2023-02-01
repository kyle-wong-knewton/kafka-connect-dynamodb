package com.trustpilot.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Arrays;

import static com.trustpilot.connector.dynamodb.DynamoDBSourceConnectorConfig.SRC_DYNAMODB_TABLE_WHITELIST_CONFIG;
import static com.trustpilot.connector.dynamodb.DynamoDBSourceConnectorConfig.SRC_INIT_SYNC_MODE_CONFIG;

@Testcontainers
public class DynamoDBSourceConnectorIT extends KafkaConnectITBase {

    @Test
    public void onConnectorCreateRecordsAreReplicatedFromBeginningAndSwitchedToStreaming() {
        String tableName = "Movies1";
        String connector = "con1";
        CreateTableResult createTableResult = newDynamoDBTable(
                newReplicationTableRequest()
                .withTableName(tableName)
                .withKeySchema(Arrays.asList(
                        new KeySchemaElement("year", KeyType.HASH),
                        new KeySchemaElement("title", KeyType.RANGE)))
                .withAttributeDefinitions(Arrays.asList(
                        new AttributeDefinition("year", ScalarAttributeType.N),
                        new AttributeDefinition("title", ScalarAttributeType.S))));

        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2020), new KeyAttribute("title", "Tenet")),
                new Item().withKeyComponents(new KeyAttribute("year", 1999), new KeyAttribute("title", "The Matrix"))
        ));

        mockTaggingAPIResponse("/", new ResourceTags(
                Arrays.asList(createTableResult.getTableDescription().getTableArn()), SRC_DYNAMODB_TABLE_INGESTION_TAG_KEY_CONFIG)
                .asJson());

        registerConnector(connector);


        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));

            // 2 - from initial sync
            // 2 - from streams as Table streaming was enabled upon table creation
            drain(consumer, 4);
        }


        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2000), new KeyAttribute("title", "Memento"))
        ));

        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));

            // all previous messages +1 newly inserted
            drain(consumer, 5);
        }

    }

    @Test
    public void onConnectorWithTableWhitelistCreateRecordsAreReplicatedFromBeginningAndSwitchedToStreaming() {
        String tableName = "Movies2";
        String connector = "con2";
        newDynamoDBTable(
                newReplicationTableRequest()
                        .withTableName(tableName)
                        .withKeySchema(Arrays.asList(
                                new KeySchemaElement("year", KeyType.HASH),
                                new KeySchemaElement("title", KeyType.RANGE)))
                        .withAttributeDefinitions(Arrays.asList(
                                new AttributeDefinition("year", ScalarAttributeType.N),
                                new AttributeDefinition("title", ScalarAttributeType.S))));

        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2020), new KeyAttribute("title", "Tenet")),
                new Item().withKeyComponents(new KeyAttribute("year", 1999), new KeyAttribute("title", "The Matrix"))
        ));


        registerConnector(connector, Arrays.asList(tableName));


        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));

            // 2 - from initial sync
            // 2 - from streams as Table streaming was enabled upon table creation
            drain(consumer, 4);
        }


        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2000), new KeyAttribute("title", "Memento"))
        ));

        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));


            // all previous messages +1 newly inserted
            drain(consumer, 5);
        }

    }

    @Test
    public void onlyStreamNewRecordsWhenInitModeDisabled() {
        String tableName = "Movies3";
        String connector = "con3";
        // Table is created without DynamoDb streams, items are insert, then streams is enabled and
        // items are written again.
        // This will ensure that, upon connector start up, only items written to dynamo AFTER
        // streams was enabled will make it into the source connector.
        newDynamoDBTable(
                new CreateTableRequest()
                        .withProvisionedThroughput(
                                new ProvisionedThroughput()
                                    .withReadCapacityUnits(5L)
                                    .withWriteCapacityUnits(5L))
                        .withTableName(tableName)
                        .withKeySchema(Arrays.asList(
                                new KeySchemaElement("year", KeyType.HASH),
                                new KeySchemaElement("title", KeyType.RANGE)))
                        .withAttributeDefinitions(Arrays.asList(
                                new AttributeDefinition("year", ScalarAttributeType.N),
                                new AttributeDefinition("title", ScalarAttributeType.S))));

        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2020), new KeyAttribute("title", "Tenet")),
                new Item().withKeyComponents(new KeyAttribute("year", 1999), new KeyAttribute("title", "The Matrix"))
        ));

        enableDynamoDbStreams(tableName);

        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2006), new KeyAttribute("title", "The Prestige"))
        ));

        registerConnector(connector, ImmutableMap.of(SRC_DYNAMODB_TABLE_WHITELIST_CONFIG, tableName,
                                                     SRC_INIT_SYNC_MODE_CONFIG, "false"));


        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));

            // 1 - from streams after streams was enabled
            drain(consumer, 1);
        }


        putDynamoDBItems(tableName, Arrays.asList(
                new Item().withKeyComponents(new KeyAttribute("year", 2000), new KeyAttribute("title", "Memento"))
        ));

        try(KafkaConsumer<String, String> consumer = getConsumer()) {
            consumer.subscribe(Arrays.asList("dynamodb-" + tableName));


            // all previous messages +1 newly inserted
            drain(consumer, 2);
        }

    }
}
