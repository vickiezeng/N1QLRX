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
import com.couchbase.client.java.query.AsyncN1qlQueryResult;
import com.couchbase.client.java.query.AsyncN1qlQueryRow;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;








import rx.Observable;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.schedulers.Schedulers;

public class Modified_Async {

    private static Cluster cluster;
    private static Bucket bucketExt;
    private static Bucket bucketXcd;
    private static long CONNECT_TIMEOUT = 30000000;
    private static String id ;

   
    public static void main(String[] args) throws InterruptedException {
       
        // connect to a cluster reachable at localhost and get XCD bucket
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
                .connectTimeout(TimeUnit.SECONDS.toMillis(600000000)).requestBufferSize(1024).build();
        cluster = CouchbaseCluster.create(env, "localhost");
        bucketExt = cluster.openBucket("extrnitem", "",CONNECT_TIMEOUT, TimeUnit.SECONDS);
        bucketXcd= cluster.openBucket("xcd", "",CONNECT_TIMEOUT, TimeUnit.SECONDS);        
        
        CountDownLatch latch = new CountDownLatch(1); 
         
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
        System.out.println(extList.length);
        Observable
        .from(extList)
        .flatMap(new Func1<String, Observable<JsonDocument>>() {
            @Override
            public Observable<JsonDocument> call(String id) {                    
                return bucketExt.async().get(id);
            }
        }).map(new Func1<JsonDocument,String>() {
            @Override
            public String call(JsonDocument loaded) {                    
                String xcdkey="XCD::"+loaded.content().get("extrnCode")+"::"+ loaded.content().get("procGrpId")+"::4::0::0";
                String extKey="Ext::"+loaded.content().get("extrnItemId");
                //JsonDocument docNew = JsonDocument.create(key, loaded.content());
                JsonObject extItm = loaded.content();
                	String query="select meta().id from " + xcdBucketName + " USE KEYS "+'"'+xcdkey+'"';  
                    Observable<AsyncN1qlQueryResult> xcdResult = bucketXcd.async().query(N1qlQuery.simple(query));
                    xcdResult.flatMap(result ->result.errors().flatMap(e -> Observable.<AsyncN1qlQueryRow>error(new Throwable("N1QL Error/Warning: " + e)))
                    .switchIfEmpty(result.rows()))
                 .map(AsyncN1qlQueryRow::value)
                 .flatMap(new Func1 <JsonObject,Observable<JsonDocument>>(){
                     @Override
                     public Observable<JsonDocument> call(JsonObject obj) { 
                    	 if(obj.get("id").toString()!= null){
         					 extItm.put("status", "Changed");       
                     }
                    	 JsonDocument docNew = JsonDocument.create(extKey, extItm);
                    	 return  bucketExt.async().upsert(docNew);
                 }
                 
                 });
                 
//                 subscribe(new Subscriber<JsonObject>() 
//                {
//                	@Override 
//                	public void onCompleted() 
//                	{   latch.countDown();
//                		System.out.println("Complete! latch=");
//                		} 
//                	@Override 
//                	public void onError(Throwable e) {
//                		latch.countDown();
//                	}
//                	@Override 
//                	public void onNext(JsonObject value)
//                	{
//                		System.out.println(value);
//					if(value.get("id").toString()!= null){
//					
//					 extItm.put("status", "Changed");	
//					 JsonDocument docNew = JsonDocument.create(extKey, extItm);
//					 bucketExt.async().upsert(docNew);
//					 System.out.println("exist");
//					 }    	
//                }
//                });
                return "updated";
//              return  (String) extItm.get("status");
            }
        })
        .subscribe(jsondoc -> {System.out.println("json:"+jsondoc);},
                    runtimeError -> {runtimeError.printStackTrace();System.out.println("Error Count"+latch.getCount()) ; latch.countDown();},
                    () -> {System.out.println("Finished Count"+latch.getCount());latch.countDown();});
//        latch1.await();
        latch.await();
//        return Thread.currentThread().getName();
        cluster.disconnect();
    }
}




