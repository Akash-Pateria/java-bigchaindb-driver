package com.bigchaindb.smartchaindb.driver;


import java.io.IOException;
import java.security.KeyPair;
import java.util.*;

//import org.apache.jena.base.Sys;
import org.apache.kafka.common.protocol.types.Field;
import org.json.JSONObject;

import com.bigchaindb.builders.BigchainDbConfigBuilder;
import com.bigchaindb.builders.BigchainDbTransactionBuilder;
import com.bigchaindb.constants.Operations;
import com.bigchaindb.model.FulFill;
import com.bigchaindb.model.GenericCallback;
import com.bigchaindb.model.MetaData;
import com.bigchaindb.model.Transaction;
import com.bigchaindb.util.Base58;

import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import okhttp3.Response;

/**
 * simple usage of BigchainDB Java driver (https://github.com/bigchaindb/java-bigchaindb-driver)
 * to create TXs on BigchainDB network
 * @author dev@bigchaindb.com
 *
 */
public class BigchainDBJavaDriver {

    /**
     * main method
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String args[]) throws Exception {

        BigchainDBJavaDriver examples = new BigchainDBJavaDriver();

        //set configuration
        BigchainDBJavaDriver.setConfig();

        //generate Keys
        KeyPair keys = BigchainDBJavaDriver.getKeys();

        System.out.println(Base58.encode(keys.getPublic().getEncoded()));
        System.out.println(Base58.encode(keys.getPrivate().getEncoded()));

        // create New asset for create txn
        Map<String, String> cre_assetData = new TreeMap<String, String>() {{
            put("name", "ABC Manufacturer");
            put("capability", "Plastic Machining");
            put("capacity", "1000");
        }};

        // create metadata for create txn
        MetaData cre_metaData = new MetaData();
        cre_metaData.setMetaData("about", "ABC plastic manufacturing company");

        //execute CREATE transaction
        String txId_cre = examples.doCreate(cre_assetData, cre_metaData, keys);

        System.out.println("Create txn id: "+ txId_cre );


        //let the transaction commit in block
        Thread.sleep(500);

        for(int i=0;i<50;i++) {
            // create metadata for request txn
            MetaData req_metaData = new MetaData();
            req_metaData.setMetaData("Quantity", stardogTest.getQuantity());
            req_metaData.setMetaData("Material", stardogTest.getMaterial());
//          req_metaData.setMetaData("Machining Function", stardogTest.getMachiningFunction());

            //execute REQUEST transaction
            List<String> randomAtributes = stardogTest.getKeys();
//            System.out.println(randomAtributes.size());
            for (int j = 0; j < randomAtributes.size(); j++) {
                String temp = randomAtributes.get(j);
//                System.out.println("KEY FOR METADATA --------- " + temp);
                req_metaData.setMetaData(temp, stardogTest.getRandomValues(temp));
            }
            List<String> capability;
            Map<String, String> metaMap= req_metaData.getMetadata();
            List<String> attributes = new ArrayList<>(metaMap.keySet());
            capability = rulesDriver.getCapabilities(attributes,metaMap);


            String txId_req = examples.doRequest(req_metaData, keys, capability);
            System.out.println("Request txn id: " + txId_req);
//            Thread.sleep(15000);
        }

//----------------------------------------------------------------------------------
        //     System.out.println("(*) Metadata Prepared..");
        //   System.out.println("(*) Assets Prepared..");
        //let the transaction commit in block
        //   Thread.sleep(500);
        //execute INTEREST transaction
        //String txId_int = examples.doInterest(txId_cre, req_metaData, keys);

        //System.out.println("Interest txn id: "+ txId_int );

//        Map<String, String> assetData1 = new TreeMap<String, String>() {{
//            put("", "");
//        }};
//
//        Map<String, String> assetData2 = new TreeMap<String, String>() {{
//            put("name", "Punnag");
//            put("age", "doesn't matter");
//            put("purpose", "masters student");
//        }};
//
//        MetaData metaData1 = new MetaData();
//        metaData1.setMetaData("where is she now?", "Raleigh");
//
//        MetaData metaData2 = new MetaData();
//        metaData2.setMetaData("where is she now?", "Raleigh");

//        String txId1 = examples.doCreate(assetData1, metaData1, keys);
//        String txId2 = examples.doCreate(assetData2, metaData2, keys);

        //create transfer metadata
//        MetaData transferMetadata = new MetaData();
//        transferMetadata.setMetaData("type", "Trial transfer");
//        System.out.println("(*) Transfer Metadata Prepared..");
//        //execute TRANSFER transaction on the CREATED asset
//        examples.doTransfer(txId_cre, transferMetadata, keys);
        //       String txId_req = examples.doRequest(req_metaData, keys);
//--------------------------------------------------------------------------------------

    }

    private void onSuccess(Response response) {
        //TODO : Add your logic here with response from server
        System.out.println("Transaction posted successfully");
    }

    private void onFailure() {
        //TODO : Add your logic here
        System.out.println("Transaction failed");
    }

    private GenericCallback handleServerResponse(String operation, MetaData metadata, String tx_id, List<String> capability) {
        //define callback methods to verify response from BigchainDBServer
        GenericCallback callback = new GenericCallback() {

            public void transactionMalformed(Response response) {
                System.out.println("malformed " + response.message());
                onFailure();
            }

            public void pushedSuccessfully(Response response) {
                if(operation.equals("REQUEST_FOR_QUOTE")) {

                    Map<String, String> metaMap= metadata.getMetadata();
//                    String material = metaMap.get("Material");
//                    int quantity = Integer.parseInt(metaMap.get("Quantity"));
//                    List<String> attributes = new ArrayList<>(metaMap.keySet());
//                    for(int i=0;i<attributes.size();i++){
//                        System.out.println("keys --- "+attributes.get(i));
//                    }
                    JSONObject js = new JSONObject(metaMap);
//                    List<String> capability;

                    //Rules for topic selection
//                    if(material != null && material.equalsIgnoreCase("PolyCarbonate")) {
//                        if(quantity < 1000){
//                            capability.add(Capabilities.PRINTING_3D);
//                            capability.add(Capabilities.POCKET_MACHINING);
//                        }
//                        else {
//                            capability.add(Capabilities.PLASTIC);
//                            capability.add(Capabilities.MILLING);
//                            capability.add(Capabilities.THREADING);
//                        }
//                    }
//                    else{
//                        capability.add(Capabilities.MISC);
//                    }
//                    capability = rulesDriver.getCapabilities(attributes,metaMap);

                    //Need to tag each capability with an integer.
                    js.put("Capability", capability);
                    js.put("Transaction_id",tx_id);
                    String rfq_form = js.toString();

                    KafkaDriver kf = new KafkaDriver(rfq_form);

                    for(String topic:capability) {
                        kf.runProducer(topic);
                    }

                    System.out.println("Producer run complete");

                }
                System.out.println(operation +" transaction pushed Successfully");
                onSuccess(response);
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
        BigchainDbConfigBuilder
                .baseUrl("http://152.46.16.77:9984/").setup(); //or use http://testnet.bigchaindb.com or https://test.bigchaindb.com/ for testnet
        //   .addToken("app_id", "ce0575bf")
        //   .addToken("app_key", "f45db167dd8ea3cf565b1d5f9cf6fa48").setup();

    }
    /**
     * generates EdDSA keypair to sign and verify transactions
     * @return KeyPair
     */
    public static KeyPair getKeys() {
        //  prepare your keys
        net.i2p.crypto.eddsa.KeyPairGenerator edDsaKpg = new net.i2p.crypto.eddsa.KeyPairGenerator();
        KeyPair abhisha = edDsaKpg.generateKeyPair();
        System.out.println("(*) Keys Generated..");
        return abhisha;

    }

    /**
     * performs CREATE transactions on BigchainDB network
     * @param assetData data to store as asset
     * @param metaData data to store as metadata
     * @param keys keys to sign and verify transaction
     * @return id of CREATED asset
     */
    public String doCreate(Map<String, String> assetData, MetaData metaData, KeyPair keys) throws Exception {

        try {
            //build and send CREATE transaction
            Transaction transaction = null;

            BigchainDbTransactionBuilder.IBuild temp = BigchainDbTransactionBuilder
                    .init()
                    .addAssets(assetData, TreeMap.class)
                    .addMetaData(metaData)
                    .operation(Operations.CREATE)
                    .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate());

            transaction = temp.getTransaction();
            List<String> cap = null;
            transaction = temp.sendTransaction(handleServerResponse("CREATE", metaData,transaction.getId(), cap));
//            Thread.sleep(10000);
            System.out.println("(*) CREATE Transaction sent.. - " + transaction.getId());
            return transaction.getId();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }

//    /**
//     * performs TRANSFER operations on CREATED assets
//     * @param txId id of transaction/asset
//     * @param metaData data to append for this transaction
//     * @param keys keys to sign and verify transactions
//     */
//    public void doTransfer(String txId, MetaData metaData, KeyPair keys) throws Exception {
//
//        Map<String, String> assetData = new TreeMap<String, String>();
//        assetData.put("id", txId);
//
//        try {
//
//
//            //which transaction you want to fulfill?
//            FulFill fulfill = new FulFill();
//            fulfill.setOutputIndex(0);
//            fulfill.setTransactionId(txId);
//
//
//            //build and send TRANSFER transaction
//            Transaction transaction = BigchainDbTransactionBuilder
//                    .init()
//                    .addInput(null, fulfill, (EdDSAPublicKey) keys.getPublic())
//                    .addOutput("1", (EdDSAPublicKey) keys.getPublic())
//                    .addAssets(txId, String.class)
//                    .addMetaData(metaData)
//                    .operation(Operations.TRANSFER)
//                    .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate())
//                    .sendTransaction(handleServerResponse("TRANSFER", metaData));
//
//            System.out.println("(*) TRANSFER Transaction sent.. - " + transaction.getId());
//
//
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//    }

    public String doRequest(MetaData metaData, KeyPair keys, List<String> capability) throws Exception {
        //Creating empty asset for REQUEST_FOR_QUOTE transaction
        Map<String, String> assetData = new TreeMap<String, String>() {{
            put("", "");
        }};

        try {
            //build and send REQUEST transaction
            Transaction transaction = null;
            BigchainDbTransactionBuilder.IBuild temp = BigchainDbTransactionBuilder
                                .init()
                                .addAssets(assetData, TreeMap.class)
                                .addMetaData(metaData)
                                .operation(Operations.REQUEST_FOR_QUOTE)
                                .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate());

            transaction = temp.getTransaction();
//            System.out.println("Id" + transaction.getId());
            transaction = temp.sendTransaction(handleServerResponse("REQUEST_FOR_QUOTE", metaData, transaction.getId(),capability));

            System.out.println("(*) REQUEST Transaction sent.. - " + transaction.getId());
            return transaction.getId();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }
//
//    public String doInterest(String txId, MetaData metaData, KeyPair keys) throws Exception {
//
//    	Map<String, String> assetData = new TreeMap<String, String>();
//        assetData.put("id", txId);
//
//        try {
//        //build and send REQUEST transaction
//        Transaction transaction = null;
//
//             transaction = BigchainDbTransactionBuilder
//                    .init()
//                    .addAssets(assetData, TreeMap.class)
//                    .addMetaData(metaData)
//                    .operation(Operations.INTEREST)
//                    .buildAndSign((EdDSAPublicKey) keys.getPublic(), (EdDSAPrivateKey) keys.getPrivate())
//                    .sendTransaction(handleServerResponse("INTEREST", metaData));
//
//            System.out.println("(*) INTEREST Transaction sent.. - " + transaction.getId());
//            return transaction.getId();
//
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//
//        return null;
//    }
}
