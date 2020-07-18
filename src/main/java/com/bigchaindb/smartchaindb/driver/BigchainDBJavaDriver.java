package com.bigchaindb.smartchaindb.driver;

import java.io.IOException;
import java.security.KeyPair;
import java.util.*;
import java.time.LocalDateTime;

//import org.apache.jena.base.Sys;
import org.json.JSONObject;

import kafka.server.ProduceMetadata;

import com.bigchaindb.builders.BigchainDbConfigBuilder;
import com.bigchaindb.builders.BigchainDbTransactionBuilder;
import com.bigchaindb.constants.Operations;
import com.bigchaindb.model.GenericCallback;
import com.bigchaindb.model.MetaData;
import com.bigchaindb.model.Transaction;
import com.bigchaindb.util.Base58;

import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import okhttp3.Response;

/**
 * simple usage of BigchainDB Java driver
 * (https://github.com/bigchaindb/java-bigchaindb-driver) to create TXs on
 * BigchainDB network
 * 
 * @author dev@bigchaindb.com
 *
 */
public class BigchainDBJavaDriver {

    /**
     * main method
     * 
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String args[]) throws Exception {

        // Main to create number of requests (Bighain Metadata) and assign topics for
        // those requests and sending it through the kafka.
        BigchainDBJavaDriver examples = new BigchainDBJavaDriver();

        // set configuration
        BigchainDBJavaDriver.setConfig();
        // generate Keys
        KeyPair keys = BigchainDBJavaDriver.getKeys();

        System.out.println(Base58.encode(keys.getPublic().getEncoded()));
        System.out.println(Base58.encode(keys.getPrivate().getEncoded()));

        // create New asset for create txn
        Map<String, String> cre_assetData = new TreeMap<String, String>() {
            {
                put("name", "ABC Manufacturer");
                put("capability", "Plastic Machining");
                put("capacity", "10000000");
            }
        };

        // create metadata for create txn
        MetaData cre_metaData = new MetaData();
        cre_metaData.setMetaData("about", "ABC plastic manufacturing company");

        // execute CREATE transaction
        examples.doCreate(cre_assetData, cre_metaData, keys);

        // let the transaction commit in block
        Thread.sleep(1000);

        // hashmap to store the topics and its id's
        HashMap<String, Integer> topicToIdMap = new HashMap<>();
        // topics to assign the requests
        topicToIdMap = CoordinatorDriver.getIdForTopics(topicToIdMap);

        final int threadCount = 5;

        for (int i = 0; i < threadCount; i++) {
            final Thread thread = new Thread(new ParallelProducers(examples, keys), "Producer-" + (i + 1));
            thread.start();
        }

        // simulateExecution(examples, keys);
        // DBConnectionPool.destroyConnectionPool();
    }

    private static class ParallelProducers implements Runnable {
        private final BigchainDBJavaDriver examples;
        private final KeyPair keys;

        ParallelProducers(BigchainDBJavaDriver driver, KeyPair keyPairs) {
            examples = driver;
            keys = keyPairs;
        }

        @Override
        public void run() throws RuntimeException {
            int numOfRequest = 2000;
            int maxProductCountInRequest = 3;
            Random random = new Random();

            for (int i = 0; i < numOfRequest; i++) {
                System.out.println("\n\nProcessing request#" + (i + 1));
                int productCount = random.nextInt(maxProductCountInRequest) + 1;
                String quantity = StardogTest.getQuantity();
                String material = StardogTest.getMaterial();
                Set<String> allCapability = new HashSet<>();
                List<Map<String, String>> productsList = new ArrayList<>();

                MetaData reqMetaData = new MetaData();
                // reqMetaData.setMetaData("Quantity", quantity);
                // reqMetaData.setMetaData("Material", material);

                for (int k = 0; k < productCount; k++) {
                    Map<String, String> productMetadata = new TreeMap<String, String>();
                    List<String> randomAtributes = StardogTest.getKeys();

                    productMetadata.put("Quantity", quantity);
                    productMetadata.put("Material", material);
                    for (int j = 0; j < randomAtributes.size(); j++) {
                        String key = randomAtributes.get(j);
                        productMetadata.put(key, StardogTest.getRandomValues(key));
                    }

                    List<String> attributes = new ArrayList<>(productMetadata.keySet());
                    List<String> capability = RulesDriver.getCapabilities(attributes, productMetadata);
                    allCapability.addAll(capability);
                    productsList.add(productMetadata);
                }

                System.out.println("Inferred Capabilities: " + allCapability.toString());
                reqMetaData.setMetaData("products", productsList);

                try {
                    examples.doRequest(reqMetaData, keys, new ArrayList<>(allCapability));
                    Thread.sleep(200);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void simulateExecution(BigchainDBJavaDriver driver, KeyPair keys) throws Exception {
        int maxProductCountInRequest = 3;
        Random random = new Random();

        MetaData reqMetaData = new MetaData();
        reqMetaData.setMetaData("Material", "SiliconCarbide");
        reqMetaData.setMetaData("ODCutOffSolid", "name");
        reqMetaData.setMetaData("GeneralClosedPocket", "minRadiusInConcaveCorner");
        reqMetaData.setMetaData("Workpiece", "Seat");

        for (;;) {
            int productCount = random.nextInt(maxProductCountInRequest) + 1;
            List<String> capabilityTopics = new ArrayList<>();

            for (int i = 0; i < productCount; i++) {
                List<String> capabilities = StardogTest.getRandomCapability();

                for (String capability : capabilities) {
                    capabilityTopics.add(StardogTest.getCapabilityTopic(capability));
                }
            }

            reqMetaData.setMetaData("Quantity", Integer.toString(random.nextInt(10000)));

            driver.doRequest(reqMetaData, keys, capabilityTopics);
            System.out.println("\n");
            Thread.sleep(5000);
        }
    }

    private void onSuccess(Response response) {
        // TODO : Add your logic here with response from server
        System.out.println("Transaction posted successfully");
    }

    private void onFailure() {
        // TODO : Add your logic here
        System.out.println("Transaction failed");
    }

    private GenericCallback handleServerResponse(String operation, MetaData metadata, String tx_id,
            List<String> capability) {
        // define callback methods to verify response from BigchainDBServer
        GenericCallback callback = new GenericCallback() {

            public void transactionMalformed(Response response) {
                System.out.println("malformed " + response.message());
                onFailure();
            }

            public void pushedSuccessfully(Response response) {
                if (operation.equals("REQUEST_FOR_QUOTE")) {

                    Map<String, Object> metaMap = metadata.getMetadata();
                    // String material = metaMap.get("Material");
                    // int quantity = Integer.parseInt(metaMap.get("Quantity"));
                    // List<String> attributes = new ArrayList<>(metaMap.keySet());
                    // for(int i=0;i<attributes.size();i++){
                    // System.out.println("keys --- "+attributes.get(i));
                    // }
                    JSONObject js = new JSONObject(metaMap);
                    // List<String> capability;

                    // Rules for topic selection
                    // if(material != null && material.equalsIgnoreCase("PolyCarbonate")) {
                    // if(quantity < 1000){
                    // capability.add(Capabilities.PRINTING_3D);
                    // capability.add(Capabilities.POCKET_MACHINING);
                    // }
                    // else {
                    // capability.add(Capabilities.PLASTIC);
                    // capability.add(Capabilities.MILLING);
                    // capability.add(Capabilities.THREADING);
                    // }
                    // }
                    // else{
                    // capability.add(Capabilities.MISC);
                    // }
                    // capability = rulesDriver.getCapabilities(attributes,metaMap);

                    // Need to tag each capability with an integer.
                    js.put("Capability", capability);
                    js.put("Transaction_id", tx_id);
                    js.put("kafkaInTimestamp", LocalDateTime.now());
                    String rfq_form = js.toString();

                    if (!capability.isEmpty()) {
                        KafkaDriver kf = new KafkaDriver(rfq_form, tx_id);
                        // for each topic in the request, it sends the request to the kafka driver.
                        kf.runProducer(capability.get(0));
                    }
                }
                // System.out.println(operation + " transaction pushed Successfully");
                // onSuccess(response);
            }

            public void otherError(Response response) {
                System.out.println("otherError" + response.message());
                onFailure();
            }
        };

        return callback;
    }

    /**
     * configures connection url and credentials
     */
    public static void setConfig() {
        BigchainDbConfigBuilder.baseUrl("http://152.46.17.69:9984/").setup(); // or use http://testnet.bigchaindb.com or
                                                                              // https://test.bigchaindb.com/ for
                                                                              // testnet
        // .addToken("app_id", "ce0575bf")
        // .addToken("app_key", "f45db167dd8ea3cf565b1d5f9cf6fa48").setup();

    }

    /**
     * generates EdDSA keypair to sign and verify transactions
     * 
     * @return KeyPair
     */
    public static KeyPair getKeys() {
        // prepare your keys
        net.i2p.crypto.eddsa.KeyPairGenerator edDsaKpg = new net.i2p.crypto.eddsa.KeyPairGenerator();
        KeyPair abhisha = edDsaKpg.generateKeyPair();
        System.out.println("(*) Keys Generated..");
        return abhisha;

    }

    /**
     * performs CREATE transactions on BigchainDB network
     * 
     * @param assetData data to store as asset
     * @param metaData  data to store as metadata
     * @param keys      keys to sign and verify transaction
     * @return id of CREATED asset
     */
    public String doCreate(Map<String, String> assetData, MetaData metaData, KeyPair keys) throws Exception {

        try {
            // build and send CREATE transaction
            Transaction transaction = null;

            BigchainDbTransactionBuilder.IBuild temp = BigchainDbTransactionBuilder.init()
                    .addAssets(assetData, TreeMap.class).addMetaData(metaData).operation(Operations.CREATE)
                    .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate());

            transaction = temp.getTransaction();
            List<String> cap = null;
            transaction = temp.sendTransaction(handleServerResponse("CREATE", metaData, transaction.getId(), cap));
            // Thread.sleep(10000);
            System.out.println("(*) CREATE Transaction sent.. - " + transaction.getId());
            return transaction.getId();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    // /**
    // * performs TRANSFER operations on CREATED assets
    // * @param txId id of transaction/asset
    // * @param metaData data to append for this transaction
    // * @param keys keys to sign and verify transactions
    // */
    // public void doTransfer(String txId, MetaData metaData, KeyPair keys) throws
    // Exception {
    //
    // Map<String, String> assetData = new TreeMap<String, String>();
    // assetData.put("id", txId);
    //
    // try {
    //
    //
    // //which transaction you want to fulfill?
    // FulFill fulfill = new FulFill();
    // fulfill.setOutputIndex(0);
    // fulfill.setTransactionId(txId);
    //
    //
    // //build and send TRANSFER transaction
    // Transaction transaction = BigchainDbTransactionBuilder
    // .init()
    // .addInput(null, fulfill, (EdDSAPublicKey) keys.getPublic())
    // .addOutput("1", (EdDSAPublicKey) keys.getPublic())
    // .addAssets(txId, String.class)
    // .addMetaData(metaData)
    // .operation(Operations.TRANSFER)
    // .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey)
    // keys.getPrivate())
    // .sendTransaction(handleServerResponse("TRANSFER", metaData));
    //
    // System.out.println("(*) TRANSFER Transaction sent.. - " +
    // transaction.getId());
    //
    //
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    // }

    public String doRequest(MetaData metaData, KeyPair keys, List<String> capability) throws Exception {
        // Creating empty asset for REQUEST_FOR_QUOTE transaction
        Map<String, String> assetData = new TreeMap<String, String>() {
            {
                put("", "");
            }
        };

        try {
            // build and send REQUEST transaction
            Transaction transaction = null;
            metaData.setMetaData("requestCreationTimestamp", LocalDateTime.now().toString());
            BigchainDbTransactionBuilder.IBuild temp = BigchainDbTransactionBuilder.init()
                    .addAssets(assetData, TreeMap.class).addMetaData(metaData).operation(Operations.REQUEST_FOR_QUOTE)
                    .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate());

            transaction = temp.getTransaction();
            // System.out.println("Id" + transaction.getId());
            transaction = temp.sendTransaction(
                    handleServerResponse("REQUEST_FOR_QUOTE", metaData, transaction.getId(), capability));

            System.out.println("(*) REQUEST Transaction sent.. - " + transaction.getId());
            return transaction.getId();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

    // public String doInterest(String txId, MetaData metaData, KeyPair keys) throws
    // Exception {
    //
    // Map<String, String> assetData = new TreeMap<String, String>();
    // assetData.put("id", txId);
    //
    // try {
    // //build and send REQUEST transaction
    // Transaction transaction = null;
    //
    // transaction = BigchainDbTransactionBuilder
    // .init()
    // .addAssets(assetData, TreeMap.class)
    // .addMetaData(metaData)
    // .operation(Operations.INTEREST)
    // .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey)
    // keys.getPrivate())
    // .sendTransaction(handleServerResponse("INTEREST", metaData));
    //
    // System.out.println("(*) INTEREST Transaction sent.. - " +
    // transaction.getId());
    // return transaction.getId();
    //
    // } catch (IOException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    //
    // return null;
    // }
}
