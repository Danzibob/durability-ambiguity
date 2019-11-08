package com.couchbase.samples.durability_ambiguity;

import com.couchbase.client.core.error.CASMismatchException;
import com.couchbase.client.core.error.DurabilityAmbiguousException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutationResult;

import com.couchbase.transactions.*;
import com.couchbase.transactions.config.TransactionConfig;
import com.couchbase.transactions.config.TransactionConfigBuilder;
import com.couchbase.transactions.error.TransactionFailed;
import com.couchbase.transactions.log.*;

// import com.couchbase.client.java.kv.;
import java.lang.Thread;
import java.util.Iterator;
import java.util.UUID;
import java.util.logging.*;
/**
 * To force a durability ambiguous error, try
 * to durable upsert a key to an active node,
 * where the relevant replica node is down.
 * This throws ambiguous, but the write won't
 * ever actually take place.
 */
public class Client {
    private Cluster cluster;
    private Bucket bucket;
    private Collection collection;
    private Transactions transactions;

    public Client() {
        System.out.println("Hello World!");
        cluster = Cluster.connect("couchbase://10.112.193.103", "Administrator", "password");
        bucket = cluster.bucket("default");
        collection = bucket.defaultCollection();
        TransactionConfig conf = TransactionConfigBuilder.create()
            .durabilityLevel(TransactionDurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER)
            .build();
        transactions = Transactions.create(cluster, conf);
    }

    public MutationResult setupDocs(String key, int bal) {
        // initial document upsert
        JsonArray mods = JsonArray.create();
        JsonObject doc = JsonObject.create()
            .put("name", "Jeff")
            .put("balance", bal)
            .put("mods", mods);
        return collection.upsert(key, doc);
    }

    // Without extra constructs this is what we're limited to

    public void addBalance_Durable(String key, int amount){
        // Get current document
        GetResult res = collection.get(key);
        JsonObject doc = res.contentAsObject();

        // Increase balance
        int newBalance = doc.getInt("balance") + amount;
        doc.put("balance", newBalance);

        // Set replace options
        ReplaceOptions rep_opts = ReplaceOptions.replaceOptions()
            .cas(res.cas())
            .durability(DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
        
        try {
            // Attempt a durable replace operation
            collection.replace(key, doc, rep_opts);

        } catch (DurabilityAmbiguousException ambg_ex) {

            System.out.println("Durability Ambiguous Exception");
            // Wait for operation to propegate if it was just slow

            try { Thread.sleep(1000); }
            catch (InterruptedException int_ex) {
                System.out.println("Interrupted while waiting");
            }

            // Try the operation again w/o durability but still w/ initial cas
            rep_opts.durability(DurabilityLevel.NONE);
            try {
                collection.replace(key, doc, rep_opts);
                // If this succeeded, the doc was not modified - is now (and can't be again by the delayed durable call b/c cas)
                // However this almost certainly means the durability requirements are NOT MET; eg maybe a replica node is unreachable, or disk queue is full
                // Should run cluster diagnostics to check for connectivity problems or a node dropping out
            } catch (CASMismatchException cas_ex) {
                // The doc has been modified by _someone_ - maybe us, maybe not
                // For idempotent full-document operations, advisable to do nothing here - either we succeeded or were overwritten
                // For partial modifications and non-idempotent ops, impossible to know in a general sense, requires domain knowledge and constraints
                // Even a GET would require some scenario specific analysis, but for the balance use case, totally unknowable if WE operated or not without additional fields/documents
            }/* catch ( Timeout / Temp. fail etc. ) {
                // The cluster is having issues, advise returning error to user and doing health check diagnostics
                // If necessary, make note of the ambiguous operation 
            } */
        }

    }

    // Adding a new field allows us to not compromise on durability factors

    public void addBalance_Durable_ID(String key, int amount){
        // Create a transaction ID
        String uuid = UUID.randomUUID().toString();
        // Call main function
        addBalance_Durable_ID(key, amount, uuid);
    }

    public void addBalance_Durable_ID(String key, int amount, String uuid){
        // Get current document
        GetResult initialRes = collection.get(key);
        JsonObject doc = initialRes.contentAsObject();

        // Check transaction ID isn't already in the doc (in the case of a retry)
        JsonArray mods = doc.getArray("mods");
        Iterator<Object> M = mods.iterator();
        for(String m = M.next().toString(); M.hasNext(); m = M.next().toString()){
            if(m == uuid){
                // Previously retried operation must have got through. Remove it and return success
                remove_UUID(key, uuid);
                return;
            }
        }

        // Increase balance
        int newBalance = doc.getInt("balance") + amount;
        doc.put("balance", newBalance);
        
        // Put the transaction ID in the doc
        JsonArray trs = doc.getArray("mods");
        trs.add(uuid);
        doc.put("mods", trs);

        // Set replace options
        ReplaceOptions rep_opts = ReplaceOptions.replaceOptions()
            .cas(initialRes.cas())
            .durability(DurabilityLevel.MAJORITY_AND_PERSIST_ON_MASTER);
        
        try {
            // Attempt a durable replace operation
            collection.replace(key, doc, rep_opts).cas();
            // If it succeeded, we can remove the record of the transaction (if necessary)
            // (Some systems may already keep a transaction log, which can be re-used in place of this uuid system)
            remove_UUID(key, uuid);

        } catch (DurabilityAmbiguousException ambg_ex) {

            System.out.println("Durability Ambiguous Exception");

            // Wait for operation to propegate in case it was just slow
            try { Thread.sleep(1000); }
            catch (InterruptedException int_ex) {
                System.out.println("Interrupted while waiting");
            }

            // Get the updated (or not) document
            GetResult res = collection.get(key);

            if(res.cas() != initialRes.cas()){
                // Document has been modified - check for our transaction ID:
                JsonArray mods = res.contentAsObject().getArray("mods");
                boolean found_tr = false;
                for(Iterator<Object> I = mods.iterator(); I.hasNext();){
                    if(I.next() == uuid){
                        found_tr = true;
                        break;
                    }
                }

                if(found_tr){
                    // This means our write was successful, but durability requirements may not have been met
                    // Here we could reattempt the replace without changing the values to ensure durability requirements are met,
                    // Or we could do nothing and be satisfied that the data has been committed
                    remove_UUID(key, uuid);
                } else {
                    // This means our write was not successful, and should be reattempted from the new document state
                    // Keeping the same ID means we can deal with this change being committed before we manage to retry
                    addBalance_Durable_ID(key, amount, uuid);
                }
                
            } else {
                // This means our write was not successful, and could be reattempted
                // Keeping the same ID means we can deal with this change being committed before we manage to retry 

                // However it may be that there's a larger issue at play and since we now know our change was not committed
                // (with reasonably high confidence) we could return a fail response and stop retrying
                addBalance_Durable_ID(key, amount, uuid);
            }

        } catch (CASMismatchException cas_ex) {
            // Retry the same operation starting from the current doc state
            addBalance_Durable_ID(key, amount, uuid);
        }
    }

    // Making use of SDK3.0's transactions makes the whole process MUCH simpler

    public void addBalance_Durable_Transaction(String key, int amount) {
        System.out.println("Transaction begin.");

        try {
            TransactionResult result = transactions.run((ctx) -> {
                // Get current document
                TransactionGetResult res = ctx.get(collection, key);
                JsonObject doc = res.contentAs(JsonObject.class);

                // Increase balance
                int newBalance = doc.getInt("balance") + amount;
                doc.put("balance", newBalance);
                
                // Perform replace (durability is set globally for all transaction calls)
                ctx.replace(res, doc);

                ctx.commit();
            });

            System.out.println("Transaction over.");

            for (LogDefer err : result.log().logs()) {
                System.out.println(err.toString());
            }

        } catch (TransactionFailed ex) {
            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        
    }

    private void remove_UUID(String key, String uuid) {
        GetResult res = collection.get(key);
        JsonObject doc = res.contentAsObject();
        JsonArray mods = doc.getArray("mods");
        JsonArray newMods = JsonArray.create();

        Iterator<Object> M = mods.iterator();
        for(String m = M.next().toString(); M.hasNext(); m = M.next().toString()){
            if(m != uuid){
                newMods.add(m);
            }
        }

        doc.put("mods", newMods);

        ReplaceOptions rep_opts = ReplaceOptions.replaceOptions();
        rep_opts.durability(DurabilityLevel.NONE);
        rep_opts.cas(res.cas());
        try {
            collection.replace(key, doc, rep_opts);
            System.out.println("Success");
            // Success
            return;
        } catch (CASMismatchException cas_ex) {
            System.out.println("CAS mismatch");
            // Concurrent modification; Try again from start
            remove_UUID(key, uuid);
        } catch (Exception ex) {
            // Another error occured; But durable write succeeded so still return positively
            System.out.println("Generic Error Occured While Removing Transaction Record:");
            ex.printStackTrace();
            return;
        }
    }
}
