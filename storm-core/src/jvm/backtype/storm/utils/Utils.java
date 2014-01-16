package backtype.storm.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift7.TException;
import org.json.simple.JSONValue;
import org.yaml.snakeyaml.Yaml;

import backtype.storm.Config;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.StormTopology;
import clojure.lang.IFn;
import clojure.lang.RT;

public class Utils {
    public static final String DEFAULT_STREAM_ID = "default";

    public static Object newInstance(final String klass) {
        try {
            final Class c = Class.forName(klass);
            return c.newInstance();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] serialize(final Object obj) {
        try {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserialize(final byte[] serialized) {
        try {
            final ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            final ObjectInputStream ois = new ObjectInputStream(bis);
            final Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String join(final Iterable<T> coll, final String sep) {
        final Iterator<T> it = coll.iterator();
        String ret = "";
        while (it.hasNext()) {
            ret = ret + it.next();
            if (it.hasNext()) {
                ret = ret + sep;
            }
        }
        return ret;
    }

    public static void sleep(final long millis) {
        try {
            Time.sleep(millis);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findResources(final String name) {
        try {
            final Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            final List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(final String name, final boolean mustExist) {
        try {
            final HashSet<URL> resources = new HashSet<URL>(findResources(name));
            if (resources.isEmpty()) {
                if (mustExist) {
                    throw new RuntimeException("Could not find config file on classpath " + name);
                } else {
                    return new HashMap();
                }
            }
            if (resources.size() > 1) {
                throw new RuntimeException("Found multiple " + name
                        + " resources. You're probably bundling the Storm jars with your topology jar. " + resources);
            }
            final URL resource = resources.iterator().next();
            final Yaml yaml = new Yaml();
            Map ret = (Map) yaml.load(new InputStreamReader(resource.openStream()));
            if (ret == null) {
                ret = new HashMap();
            }

            return new HashMap(ret);

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(final String name) {
        return findAndReadConfigFile(name, true);
    }

    public static Map readDefaultConfig() {
        return findAndReadConfigFile("defaults.yaml", true);
    }

    public static Map readCommandLineOpts() {
        final Map ret = new HashMap();
        String commandOptions = System.getProperty("storm.options");
        if (commandOptions != null) {
            commandOptions = commandOptions.replaceAll("%%%%", " ");
            final String[] configs = commandOptions.split(",");
            for (final String config : configs) {
                final String[] options = config.split("=");
                if (options.length == 2) {
                    ret.put(options[0], options[1]);
                }
            }
        }
        return ret;
    }

    public static Map readStormConfig() {
        final Map ret = readDefaultConfig();
        final String confFile = System.getProperty("storm.conf.file");
        Map storm;
        if (confFile == null || confFile.equals("")) {
            storm = findAndReadConfigFile("storm.yaml", false);
        } else {
            storm = findAndReadConfigFile(confFile, true);
        }
        ret.putAll(storm);
        ret.putAll(readCommandLineOpts());
        return ret;
    }

    private static Object normalizeConf(final Object conf) {
        if (conf == null) {
            return new HashMap();
        }
        if (conf instanceof Map) {
            final Map confMap = new HashMap((Map) conf);
            for (final Object key : confMap.keySet()) {
                final Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if (conf instanceof List) {
            final List confList = new ArrayList((List) conf);
            for (int i = 0; i < confList.size(); i++) {
                final Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if (conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }

    public static boolean isValidConf(final Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(normalizeConf(JSONValue.parse(JSONValue.toJSONString(stormConf))));
    }

    public static Object getSetComponentObject(final ComponentObject obj) {
        if (obj.getSetField() == ComponentObject._Fields.SERIALIZED_JAVA) {
            return Utils.deserialize(obj.get_serialized_java());
        } else if (obj.getSetField() == ComponentObject._Fields.JAVA_OBJECT) {
            return obj.get_java_object();
        } else {
            return obj.get_shell();
        }
    }

    public static <S, T> T get(final Map<S, T> m, final S key, final T def) {
        T ret = m.get(key);
        if (ret == null) {
            ret = def;
        }
        return ret;
    }

    public static List<Object> tuple(final Object... values) {
        final List<Object> ret = new ArrayList<Object>();
        for (final Object v : values) {
            ret.add(v);
        }
        return ret;
    }

    public static void downloadFromMaster(final Map conf, final String file, final String localFile)
            throws IOException, TException {
        final NimbusClient client = NimbusClient.getConfiguredClient(conf);
        final String id = client.getClient().beginFileDownload(file);
        final WritableByteChannel out = Channels.newChannel(new FileOutputStream(localFile));
        while (true) {
            final ByteBuffer chunk = client.getClient().downloadChunk(id);
            final int written = out.write(chunk);
            if (written == 0) {
                break;
            }
        }
        out.close();
    }

    public static IFn loadClojureFn(final String namespace, final String name) {
        try {
            clojure.lang.Compiler.eval(RT.readString("(require '" + namespace + ")"));
        } catch (final Exception e) {
            // if playing from the repl and defining functions, file won't exist
        }
        return (IFn) RT.var(namespace, name).deref();
    }

    public static boolean isSystemId(final String id) {
        return id.startsWith("__");
    }

    public static <K, V> Map<V, K> reverseMap(final Map<K, V> map) {
        final Map<V, K> ret = new HashMap<V, K>();
        for (final K key : map.keySet()) {
            ret.put(map.get(key), key);
        }
        return ret;
    }

    public static ComponentCommon getComponentCommon(final StormTopology topology, final String id) {
        if (topology.get_spouts().containsKey(id)) {
            return topology.get_spouts().get(id).get_common();
        }
        if (topology.get_bolts().containsKey(id)) {
            return topology.get_bolts().get(id).get_common();
        }
        if (topology.get_state_spouts().containsKey(id)) {
            return topology.get_state_spouts().get(id).get_common();
        }
        throw new IllegalArgumentException("Could not find component with id " + id);
    }

    public static Integer getInt(final Object o) {
        if (o instanceof Long) {
            return ((Long) o).intValue();
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Short) {
            return ((Short) o).intValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to int");
        }
    }

    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }

    public static CuratorFramework newCurator(final Map conf, final List<String> servers, final Object port,
            final String root) {
        return newCurator(conf, servers, port, root, null);
    }

    public static class BoundedExponentialBackoffRetry extends ExponentialBackoffRetry {

        protected final int maxRetryInterval;

        public BoundedExponentialBackoffRetry(final int baseSleepTimeMs, final int maxRetries, final int maxSleepTimeMs) {
            super(baseSleepTimeMs, maxRetries);
            this.maxRetryInterval = maxSleepTimeMs;
        }

        public int getMaxRetryInterval() {
            return this.maxRetryInterval;
        }

        @Override
        public int getSleepTimeMs(final int count, final long elapsedMs) {
            return Math.min(maxRetryInterval, super.getSleepTimeMs(count, elapsedMs));
        }

    }

    public static CuratorFramework newCurator(final Map conf, final List<String> servers, final Object port,
            final String root, final ZookeeperAuthInfo auth) {
        final List<String> serverPorts = new ArrayList<String>();
        for (final String zkServer : servers) {
            serverPorts.add(zkServer + ":" + Utils.getInt(port));
        }
        final String zkStr = StringUtils.join(serverPorts, ",") + root;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
                .builder()
                .connectString(zkStr)
                .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
                .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
                .retryPolicy(
                        new BoundedExponentialBackoffRetry(
                                Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)), Utils.getInt(conf
                                        .get(Config.STORM_ZOOKEEPER_RETRY_TIMES)), Utils.getInt(conf
                                        .get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING))));
        if (auth != null && auth.scheme != null) {
            builder = builder.authorization(auth.scheme, auth.payload);
        }
        return builder.build();
    }

    public static CuratorFramework newCurator(final Map conf, final List<String> servers, final Object port) {
        return newCurator(conf, servers, port, "");
    }

    public static CuratorFramework newCuratorStarted(final Map conf, final List<String> servers, final Object port,
            final String root) {
        final CuratorFramework ret = newCurator(conf, servers, port, root);
        ret.start();
        return ret;
    }

    public static CuratorFramework newCuratorStarted(final Map conf, final List<String> servers, final Object port) {
        final CuratorFramework ret = newCurator(conf, servers, port);
        ret.start();
        return ret;
    }

    /**
     * 
     (defn integer-divided [sum num-pieces] (let [base (int (/ sum num-pieces)) num-inc (mod sum num-pieces) num-bases
     * (- num-pieces num-inc)] (if (= num-inc 0) {base num-bases} {base num-bases (inc base) num-inc} )))
     * 
     * @param sum
     * @param numPieces
     * @return
     */

    public static TreeMap<Integer, Integer> integerDivided(final int sum, final int numPieces) {
        final int base = sum / numPieces;
        final int numInc = sum % numPieces;
        final int numBases = numPieces - numInc;
        final TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if (numInc != 0) {
            ret.put(base + 1, numInc);
        }
        return ret;
    }

    public static byte[] toByteArray(final ByteBuffer buffer) {
        final byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }

    public static boolean exceptionCauseIsInstanceOf(final Class klass, final Throwable throwable) {
        Throwable t = throwable;
        while (t != null) {
            if (klass.isInstance(t)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
