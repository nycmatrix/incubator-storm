package storm.trident.topology.state;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.json.simple.JSONValue;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

public class TransactionalState {
    CuratorFramework _curator;

    public static TransactionalState newUserState(final Map conf, final String id) {
        return new TransactionalState(conf, id, "user");
    }

    public static TransactionalState newCoordinatorState(final Map conf, final String id) {
        return new TransactionalState(conf, id, "coordinator");
    }

    protected TransactionalState(Map conf, final String id, final String subroot) {
        try {
            conf = new HashMap(conf);
            final String rootDir = conf.get(Config.TRANSACTIONAL_ZOOKEEPER_ROOT) + "/" + id + "/" + subroot;
            final List<String> servers = (List<String>) getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_SERVERS,
                    Config.STORM_ZOOKEEPER_SERVERS);
            final Object port = getWithBackup(conf, Config.TRANSACTIONAL_ZOOKEEPER_PORT, Config.STORM_ZOOKEEPER_PORT);
            final CuratorFramework initter = Utils.newCuratorStarted(conf, servers, port);
            try {
                initter.create().creatingParentsIfNeeded().forPath(rootDir);
            } catch (final KeeperException.NodeExistsException e) {

            }

            initter.close();

            _curator = Utils.newCuratorStarted(conf, servers, port, rootDir);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setData(String path, final Object obj) {
        path = "/" + path;
        byte[] ser;
        try {
            ser = JSONValue.toJSONString(obj).getBytes("UTF-8");
        } catch (final UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        try {
            if (_curator.checkExists().forPath(path) != null) {
                _curator.setData().forPath(path, ser);
            } else {
                _curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, ser);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(String path) {
        path = "/" + path;
        try {
            _curator.delete().forPath(path);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> list(String path) {
        path = "/" + path;
        try {
            if (_curator.checkExists().forPath(path) == null) {
                return new ArrayList<String>();
            } else {
                return _curator.getChildren().forPath(path);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void mkdir(final String path) {
        setData(path, 7);
    }

    public Object getData(String path) {
        path = "/" + path;
        try {
            if (_curator.checkExists().forPath(path) != null) {
                return JSONValue.parse(new String(_curator.getData().forPath(path), "UTF-8"));
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        _curator.close();
    }

    private Object getWithBackup(final Map amap, final Object primary, final Object backup) {
        final Object ret = amap.get(primary);
        if (ret == null) {
            return amap.get(backup);
        }
        return ret;
    }
}
