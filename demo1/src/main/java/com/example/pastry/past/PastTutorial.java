package com.example.pastry.past;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.Vector;
import java.util.function.Predicate;

import com.example.newpastry.NewPastImpl;
import com.fasterxml.jackson.annotation.JsonProperty;
import rice.Continuation;
import rice.environment.Environment;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.past.*;
import rice.pastry.*;
import rice.pastry.commonapi.PastryIdFactory;
import rice.pastry.socket.SocketPastryNodeFactory;
import rice.pastry.standard.RandomNodeIdFactory;
import rice.persistence.*;
import rice.tutorial.ssl.MyApp;
import sun.reflect.ReflectionFactory;

/**
 * This tutorial shows how to use Past.
 *
 * @author Jeff Hoye, Jim Stewart, Ansley Post
 */
public class PastTutorial {
    /**
     * this will keep track of our Past applications
     */
    Vector<Past> apps = new Vector<Past>();
    final Environment env;
    MyPastContent value;
    public PastTutorial(int bindport, InetSocketAddress bootaddress, int bootport, int numNodes, boolean isMemory, int replicationFactor) throws Exception {

        // Generate the NodeIds Randomly
        env = new Environment();
        env.getParameters().setString("nat_search_policy","never");

        NodeIdFactory nidFactory = new RandomNodeIdFactory(env);

        // construct the PastryNodeFactory, this is how we use rice.pastry.socket
        PastryNodeFactory factory = new SocketPastryNodeFactory(nidFactory,
                bindport, env);


        // loop to construct the nodes/apps
        for (int curNode = 0; curNode < numNodes; curNode++) {
            // construct a node, passing the null boothandle on the first loop will
            // cause the node to start its own ring
            PastryNode node = factory.newNode();

            // used for generating PastContent object Ids.
            // this implements the "hash function" for our DHT
            PastryIdFactory idf = new rice.pastry.commonapi.PastryIdFactory(env);

            // create a different storage root for each node
            String storageDirectory = System.getProperty("user.dir") + "/storage/storage/"+node.getLocalNodeHandle().getId().toString().substring(3,9);

            // create the persistent part
              Storage stor;

              if(isMemory){
                  stor = new MemoryStorage(idf);
              }
              else{
                  stor = new NewPersistentStorage(idf, storageDirectory, 4 * 1024 * 1024, node.getEnvironment());
              }

            Past app = new NewPastImpl(node, new StorageManagerImpl(idf, stor, new LRUCache(
                    new MemoryStorage(idf), 512 * 1024, node.getEnvironment())), replicationFactor, "");
            apps.add(app);

            node.boot(bootaddress);

            // the node may require sending several messages to fully boot into the ring
            synchronized(node) {
                while(!node.isReady() && !node.joinFailed()) {
                    // delay so we don't busy-wait
                    node.wait(500);

                    // abort if can't join
                    if (node.joinFailed()) {
                        throw new IOException("Could not join the FreePastry ring.  Reason:"+node.joinFailedReason());
                    }
                }
            }
            System.out.println("Finished creating new node " + node);
        }
        /**
        // wait 5 seconds
        env.getTimeSource().sleep(5000);

        // We could cache the idf from whichever app we use, but it doesn't matter
        PastryIdFactory localFactory = new rice.pastry.commonapi.PastryIdFactory(env);

        // Store 5 keys
        // let's do the "put" operation
        System.out.println("Storing 5 keys");
        Id[] storedKey = new Id[5];
        for(int ctr = 0; ctr < storedKey.length; ctr++) {
            // these variables are final so that the continuation can access them
            final String s = "Monish " + ctr;

            // build the past content
            final PastContent myContent = new MyPastContent(localFactory.buildId(s), s);

            // store the key for a lookup at a later point
            storedKey[ctr] = myContent.getId();

            // pick a random past appl on a random node
            Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));
            System.out.println("Inserting " + myContent + " at node "+p.getLocalNodeHandle());

            // insert the data
            p.insert(myContent, new Continuation<Boolean[], Exception>() {
                // the result is an Array of Booleans for each insert
                public void receiveResult(Boolean[] results) {
                    int numSuccessfulStores = 0;
                    for (int ctr = 0; ctr < results.length; ctr++) {
                        if (results[ctr].booleanValue())
                            numSuccessfulStores++;
                    }
                    System.out.println(myContent + " successfully stored at " +
                            numSuccessfulStores + " locations.");
                }

                public void receiveException(Exception result) {
                    System.out.println("Error storing "+myContent);
                    result.printStackTrace();
                }
            });
        }

        // wait 5 seconds
        env.getTimeSource().sleep(5000);

        // let's do the "get" operation
        System.out.println("Looking up the 5 keys");

        // for each stored key
        for (int ctr = 0; ctr < storedKey.length; ctr++) {
            final Id lookupKey = storedKey[ctr];

            // pick a random past appl on a random node
            Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));

            System.out.println("Looking up " + lookupKey + " at node "+p.getLocalNodeHandle());
            p.lookup(lookupKey, new Continuation<PastContent, Exception>() {
                public void receiveResult(PastContent result) {
                    System.out.println("Successfully looked up " + result + " for key "+lookupKey+".");
                }

                public void receiveException(Exception result) {
                    System.out.println("Error looking up "+lookupKey);
                    result.printStackTrace();
                }
            });
        }

        // wait 5 seconds
        env.getTimeSource().sleep(5000);

        // now lets see what happens when we do a "get" when there is nothing at the key
        System.out.println("Looking up a bogus key");
        final Id bogusKey = localFactory.buildId("bogus");

        // pick a random past appl on a random node
        Past p = (Past)apps.get(env.getRandomSource().nextInt(numNodes));

        System.out.println("Looking up bogus key " + bogusKey + " at node "+p.getLocalNodeHandle());
        p.lookup(bogusKey, new Continuation<PastContent, Exception>() {
            public void receiveResult(PastContent result) {
                System.out.println("Successfully looked up " + result + " for key "+bogusKey+".  Notice that the result is null.");
                env.destroy();
            }

            public void receiveException(Exception result) {
                System.out.println("Error looking up "+bogusKey);
                result.printStackTrace();
                env.destroy();
            }
        });
         **/
    }

    public PastContent getMethod(String lookupKey) throws InterruptedException {
        NewPastImpl p = (NewPastImpl) apps.get(0);
        PastryIdFactory localFactory = new rice.pastry.commonapi.PastryIdFactory(env);

        NewContinuation async = new NewContinuation();

        p.lookup(localFactory.buildId(lookupKey), new Continuation.StandardContinuation<PastContent, Exception>(async) {
            @Override
            public void receiveResult(PastContent result) {

                this.parent.receiveResult(result);
            }

            @Override
            public void receiveException(Exception result) {
                System.out.println("Error looking up "+lookupKey);
                result.printStackTrace();
            }
        });
        long current = env.getTimeSource().currentTimeMillis();
        while (async.getPastContent() == null && current+10000>=env.getTimeSource().currentTimeMillis()) {
            System.out.println(async.toString());
            continue;
        }

        return(async.getPastContent());
    }

    public String putMethod(String key, final String value){

        // these variables are final so that the continuation can access them
        final String s = value;
        PastryIdFactory localFactory = new PastryIdFactory(env);
        // build the past content
        final PastContent myContent = new MyPastContent(localFactory.buildId(key), s);

        // store the key for a lookup at a later point
//        storedKey[ctr] = myContent.getId();

        // pick a random past appl on a random node
        NewPastImpl p = (NewPastImpl) apps.get(env.getRandomSource().nextInt(apps.size()));
//        System.out.println("Inserting " + myContent + " at node "+p.getLocalNodeHandle());

        // insert the data
//        Continuation async = new Continuation<Boolean[], Exception>() {
//
//            // the result is an Array of Booleans for each insert
//            public void receiveResult(Boolean[] results) {
//                int numSuccessfulStores = 0;
//                for (int ctr = 0; ctr < results.length; ctr++) {
//                    if (results[ctr].booleanValue())
//                        numSuccessfulStores++;
//                }
//                System.out.println("[Continuation]"+
////                        myContent +
//                        " successfully stored at " +
//                        numSuccessfulStores + " locations.");
//            }
//
//            public void receiveException(Exception result) {
//                System.out.println("Error storing "+myContent);
//                result.printStackTrace();
//            }
//        };
//
//        p.insert(myContent,async);


        NewPutContinuation async1 = new NewPutContinuation();
        p.insert(myContent, new Continuation.StandardContinuation<Boolean[], Exception>(async1) {
            @Override
            public void receiveResult(Boolean[] results) {
                int numSuccessfulStores = 0;
                for (int ctr = 0; ctr < results.length; ctr++) {
                    if (results[ctr].booleanValue())
                        numSuccessfulStores++;
                }
                this.parent.receiveResult(results);
//                System.out.println("[Continuation]"+
////                        myContent +
//                        " successfully stored at " +
//                        numSuccessfulStores + " locations.");
            }

            public void receiveException(Exception result) {
                System.out.println("Error storing "+myContent);
                result.printStackTrace();
            }
        });

        long current = env.getTimeSource().currentTimeMillis();
        while (async1.getNumOfSuccess() == null
                || (!Arrays.stream(async1.getNumOfSuccess()).anyMatch(boo -> boo)
                && current+10000>=env.getTimeSource().currentTimeMillis())) {
//            System.out.println(async1.toString());
//            System.out.println("In continuation for " + key);
            continue;
        }
        return("true");
    }

    /**
     * Usage: java [-cp FreePastry- <version>.jar]
     * rice.tutorial.past.PastTutorial localbindport bootIP bootPort numNodes
     * example java rice.tutorial.past.PastTutorial 9001 pokey.cs.almamater.edu 9001 10
     */

}


