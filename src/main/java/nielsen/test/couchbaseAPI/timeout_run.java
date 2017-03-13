package nielsen.test.couchbaseAPI;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;

import rx.Observable;
import rx.functions.Func1;

public class timeout_run {

    private static Cluster cluster;
    private static Bucket bucketExt;
    private static Bucket bucketXcd;
    private static long CONNECT_TIMEOUT = 30000000;
    private static String id ;

    private class MyRunnable implements Callable<String> {

        @Override
        public String call() throws Exception {
            System.out.println("Starting:" + Thread.currentThread().getName());
            CountDownLatch latch = new CountDownLatch(2);        
            List<String> listId = new ArrayList<>();            
            
            String Status="PENDING";            
            String extBucketName = bucketExt.name();
            String xcdBucketName =bucketXcd.name();
            String query="select meta().id from " + extBucketName + " where status='"+Status+"'";            
            N1qlQueryResult extrnItmResult = bucketExt.query(N1qlQuery.simple(query));
            for (N1qlQueryRow et2 : extrnItmResult) {
                id = et2.value().get("id").toString();                
                listId.add(id);
            }            
            String[] extList = new String[listId.size()];
            extList = listId.toArray(extList);     
            System.out.println("size is"+extList.length);
            Observable
            .from(extList)
            .flatMap(new Func1<String, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(String id) {                    
                    return bucketExt.async().get(id);
                }
            }).flatMap(new Func1<JsonDocument, Observable<JsonDocument>>() {
                @Override
                public Observable<JsonDocument> call(JsonDocument loaded) {                    
                    String xcdkey="XCD::"+loaded.content().get("extrnCode")+"::"+ loaded.content().get("procGrpId")+"::4::0::0";
                    String extKey="Ext::"+loaded.content().get("extrnItemId");
                    //JsonDocument docNew = JsonDocument.create(key, loaded.content());
                    JsonObject extItm = loaded.content();
                    
//                    try{
                        //JsonDocument xcdResult = bucketXcd.get(xcdkey); 
                        String query="select meta().id from " + xcdBucketName + " USE KEYS "+'"'+xcdkey+'"';
                        System.out.println("query xcd..."+xcdkey);
                        N1qlQueryResult xcdResult = bucketXcd.query(N1qlQuery.simple(query));
                        for (N1qlQueryRow xcd : xcdResult) {
                        id = xcd.value().get("id").toString();    
                        System.out.println("Id :"+id);
                        }
                        /*if(!xcdResult.content().isEmpty()){
                            extItm.put("status", "Changed");
                        }*/
                        if(!xcdResult.allRows().isEmpty()){
                            extItm.put("status", "Changed");
                        }
//                        
//                    }
//                    catch(Exception e){
//                        System.out.println("Error:"+e.getMessage());
//                    }
                    JsonDocument docNew = JsonDocument.create(extKey, extItm);
                    return bucketExt.async().upsert(docNew);                
                }
            }).subscribe(jsondoc -> {System.out.println("json:"+jsondoc);latch.countDown();},
                    runtimeError -> {runtimeError.printStackTrace();System.out.println("Error Count"+latch.getCount()) ; latch.countDown();},
                    () -> {System.out.println("Finished Count"+latch.getCount());latch.countDown();});
            latch.await();
            return Thread.currentThread().getName();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        int numthreads = 1;
    
        // connect to a cluster reachable at localhost and get XCD bucket
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(TimeUnit.SECONDS.toMillis(600000000)).requestBufferSize(1024).build();
        cluster = CouchbaseCluster.create(env, "localhost");
        bucketExt = cluster.openBucket("extrnitem", "",CONNECT_TIMEOUT, TimeUnit.SECONDS);
        bucketXcd= cluster.openBucket("xcd", "",CONNECT_TIMEOUT, TimeUnit.SECONDS);        
        System.out.println(new Date() + "::" + "main started");
        ExecutorService executor = Executors.newFixedThreadPool(numthreads);
        List<Future<String>> list = new ArrayList<Future<String>>();
        timeout_run kv = new timeout_run();
        for (int i = 0; i < numthreads; i++) {
            Callable myRunnable = kv.new MyRunnable();
            Future<String> future = executor.submit(myRunnable);        
            list.add(future);
        }    
        executor.shutdown();        
        executor.awaitTermination(10, TimeUnit.SECONDS);        
        // Wait until all threads are finish
       while (!executor.isTerminated()) {
            
        }        
        System.out.println(new Date() + "::" + "\nFinished all threads");

        // cleanup (in a synchronous way) and disconnect
        System.out.println("Cleaning Up");
        System.out.println("Exiting");
        cluster.disconnect();
    }
}




