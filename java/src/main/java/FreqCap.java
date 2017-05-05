import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.util.TableUtils.createTableIfNotExists;
import static com.amazonaws.services.dynamodbv2.util.TableUtils.deleteTableIfExists;
import static com.amazonaws.services.dynamodbv2.util.TableUtils.waitUntilExists;

public class FreqCap {
    private final static String TABLE_NAME = "freq_cap";

    private static AmazonDynamoDBAsync session =
            AmazonDynamoDBAsyncClientBuilder.standard().withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "eu-central-1")
            ).build();

    private static void printUsage()
    {
        System.out.println("Syntax: ADD|DELETE|SHOW|CLEAR <session_id> <campaign_id>");
    }

    private static void setup_table() throws InterruptedException
    {
        List<KeySchemaElement> key_schema = new ArrayList<>();
        key_schema.add(new KeySchemaElement("id", "HASH"));
        List<AttributeDefinition> attributes = new ArrayList<>();
        attributes.add(new AttributeDefinition("id", "S"));

        createTableIfNotExists(session,
                new CreateTableRequest()
                    .withTableName(TABLE_NAME)
                    .withKeySchema(key_schema)
                    .withAttributeDefinitions(attributes)
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
        waitUntilExists(session, TABLE_NAME);
    }

    private static void add_fc(String session_id, String campaign_id) {
        UpdateItemRequest update = new UpdateItemRequest()
                .withTableName(TABLE_NAME)
                .addKeyEntry("id", new AttributeValue().withS(session_id));


        GetItemRequest get = new GetItemRequest()
                .withTableName(TABLE_NAME)
                .addKeyEntry("id", new AttributeValue().withS(session_id));

        GetItemResult itemResult = session.getItem(get);
        Map<String, AttributeValue> itemAttributes = itemResult.getItem();
        if (itemAttributes != null && itemAttributes.containsKey("campaign")) {
            Map<String, AttributeValue> v = itemAttributes.get("campaign").getM();

            AttributeValue old = v.getOrDefault(campaign_id, new AttributeValue().withN("0"));

            update
                    .addExpressionAttributeNamesEntry("#campaign", "campaign")
                    .addExpressionAttributeNamesEntry("#cid", campaign_id)
                    .addExpressionAttributeValuesEntry(":incr", new AttributeValue().withN("1"))
                    .addExpressionAttributeValuesEntry(":old", old)
                    .withUpdateExpression("SET #campaign.#cid = :old + :incr")
                    .withReturnValues(ReturnValue.ALL_NEW);

        } else {
            Map<String, AttributeValue> up = new HashMap<>();
            up.put(campaign_id, new AttributeValue().withN("1"));

            update.addAttributeUpdatesEntry("campaign",
                    new AttributeValueUpdate()
                            .withAction("PUT")
                            .withValue(new AttributeValue().withM(up)));
        }


        session.updateItemAsync(update, new AsyncHandler<UpdateItemRequest, UpdateItemResult>() {
            @Override
            public void onError(Exception exception) {
                System.out.println("Couldn't update " + session_id + ":" + campaign_id);
                System.out.println(exception.getMessage());
                System.exit(1);

            }

            @Override
            public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
                printSession(session_id);
            }
        });
    }

    private static void printSession(String session_id)
    {
        GetItemRequest ge = new GetItemRequest()
                .withTableName(TABLE_NAME)
                .addKeyEntry("id", new AttributeValue().withS(session_id));

        session.getItemAsync(ge, new AsyncHandler<GetItemRequest, GetItemResult>() {
            @Override
            public void onError(Exception exception) {
                System.out.println("Couldn't get session " + session + ": " + exception.getMessage());
                System.exit(1);
            }

            @Override
            public void onSuccess(GetItemRequest request, GetItemResult getItemResult) {
                System.out.println(request.toString());
                System.out.println(getItemResult.toString());
                System.exit(0);
            }
        });

    }


    public static void main(String args[])
    {
        if (args.length < 1) {
            FreqCap.printUsage();
            System.exit(1);
        }
        try {
            switch (args[0]) {
                case "ADD":
                    setup_table();
                    add_fc(args[1], args[2]);
                    break;
                case "DELETE":
                    setup_table();
                    del_fc(args[1], args[2]);
                    break;
                case "CLEAR":
                    deleteTableIfExists(session,
                            new DeleteTableRequest("freq_cap"));
                    break;
                case "SHOW":
                    describeTable();
                    printSession(args[1]);
                    break;
                default:
                    FreqCap.printUsage();
                    System.exit(1);
            }
        } catch (InterruptedException e) {
            ;
        }
    }

    private static void del_fc(String session_id, String campaign_id)
    {
        UpdateItemRequest update = new UpdateItemRequest()
                .withTableName(TABLE_NAME)
                .addKeyEntry("id", new AttributeValue().withS(session_id))
                .addExpressionAttributeNamesEntry("#campaign", "campaign")
                .addExpressionAttributeNamesEntry("#cid", campaign_id)
                .withUpdateExpression("REMOVE #campaign.#cid");

        session.updateItemAsync(update, new AsyncHandler<UpdateItemRequest, UpdateItemResult>() {
            @Override
            public void onError(Exception exception) {
                System.out.println("Couldn't remove campaign: " + exception.getMessage());
                System.exit(1);
            }

            @Override
            public void onSuccess(UpdateItemRequest request, UpdateItemResult updateItemResult) {
                printSession(session_id);
            }
        });
    }

    private static void describeTable()
    {
        DescribeTableResult result = session.describeTable(new DescribeTableRequest().withTableName(TABLE_NAME));
        System.out.println("Table: " + result.getTable());
    }
}