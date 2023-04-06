package com.hoddmimes.kafka.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;

import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;


import java.nio.charset.StandardCharsets;



public class CuratorClient {

    public interface PathValueChangedInterface
    {
        public void pathValueChanged( String pPath, byte[] pValue);
    }
    private CuratorFramework mClient;


    public  CuratorClient( String pConnectString ) {
        mClient = CuratorFrameworkFactory.builder()
                .connectString(pConnectString)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .build();

        mClient.start();
    }

    public void set( String pPath, String pData ) {
        try {
            mClient.setData().forPath( pPath, pData.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public boolean exists( String pPath ) {
        try {
            boolean tExists =  (mClient.checkExists().forPath(pPath) == null) ? false : true;
            System.out.println( tExists );
            return tExists;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void delete( String pPath )
    {
        try {
            mClient.delete().deletingChildrenIfNeeded().forPath(pPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void addSubscription(String pPath, PathValueChangedInterface pCallback, boolean pRecursive  ) {
        try {
            PathListener tListener = new PathListener(pPath, pCallback, pRecursive);
            if (pRecursive) {
                mClient.watchers().add().withMode(AddWatchMode.PERSISTENT_RECURSIVE).usingWatcher(tListener).forPath(pPath);
            } else {
                mClient.watchers().add().withMode(AddWatchMode.PERSISTENT).usingWatcher(tListener).forPath(pPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void removeSubscription( CuratorWatcher pWatcher ) {
        mClient.watchers().remove( pWatcher );
    }

    public void removeAllSubscription() {
        mClient.watchers().removeAll();
    }


    public byte[] get( String pPath ) {
        try {
            return mClient.getData().forPath(pPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void create( String pPath ) {
        try {
            String s = mClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(pPath);
            System.out.println(s);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class PathListener implements CuratorWatcher {
        String mPath;
        boolean mRecursive;
        PathValueChangedInterface mCallback;
        public PathListener (String pPath, PathValueChangedInterface pCallback, boolean pRecursive ) {
            mPath = pPath;
            mRecursive = pRecursive;
            mCallback = pCallback;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                byte[] tData = mClient.getData().forPath(event.getPath());
                if (mCallback != null) {
                    mCallback.pathValueChanged( event.getPath(), tData);
                }
            }
        }

    }
}
