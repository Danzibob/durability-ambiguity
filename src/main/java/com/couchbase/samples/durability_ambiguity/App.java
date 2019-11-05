package com.couchbase.samples.durability_ambiguity;

public final class App {
    private App() {
    }

    /**
     * Says hello to the world.
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        Client c = new Client();
        c.setupDocs("Jtest1");
        c.addBalance_Durable_ID("Jtest1", 5);
    }
}

/**
 * Courses of action:
 * 1) Do a get to verify operation if possible
 *      * For durability garuntees would have to check all replicas - can't select individually hence not ideal
 *      a) Settle for a regular SET / UPSERT operation -> shouldn't give ambiguous response (expt. timeout)
 * 
 * 2) Verify health of nodes
 *      * Alone doesn't check state of operation; still have to check manually
 *          * Still no way to know if WE or another client changed doc
 *              * Can check if change is intact, if y then ok (maybe) if n then ok (maybe) (?!?!)
 *      a) If a node is down and durability is essential, throw error to user
 */

 /**
  * Alternate design:
        Change balance with a subdoc mutatein, with a cas check, and a last modifications list (dict?), which must include
            The cas value the document was modified from
            A transaction uuid that is unique and recogniseable per balance change
        This allows us to check if our specific transaction went through, while still preventing it from being overwritten because of the cas checks
        These records would need to be garbage collected once read, as they need to be kept to verify any given transaction
            ie if no error arises, immediately delete the record; as soon as error is resolved, delete the record
                Could still potentially cause problems, as clients could die or lose connection and leave behind garbage.
                    Requires some sort of backup garbage collection
        https://docs.couchbase.com/java-sdk/3.0/howtos/subdocument-operations.html#durability
  */