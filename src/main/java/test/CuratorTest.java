package test;

import com.hoddmimes.kafka.curator.CuratorClient;


public class CuratorTest implements CuratorClient.PathValueChangedInterface {

    CuratorClient mClient;
    public static void main(String[] args) {
        CuratorTest ct = new CuratorTest();
        ct.test();
    }

    private void test() {
        CuratorClient cc = new CuratorClient("127.0.0.1:2181");
        if (!cc.exists("/foo/bar")) {
            cc.create("/foo/bar");
        }
        cc.addSubscription("/foo/bar", this, true);
        cc.set("/foo/bar","take the ride");
        byte[] tData = cc.get("/foo/bar");
        System.out.println("Get data value: \"" + new String( tData ) + "\"");
        cc.delete("/foo");

        while( true ) {
            try { Thread.sleep(1000L);}
            catch( InterruptedException e) {}
        }
    }
        @Override
        public void pathValueChanged(String pPath, byte[] pData ){
            System.out.println("subscription update path: " + pPath + " value: \"" + new String( pData ) + "\"");
        }

}
