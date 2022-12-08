//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.example.newpastry;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.WeakHashMap;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.params.Parameters;
import rice.p2p.commonapi.Application;
import rice.p2p.commonapi.CancellableTask;
import rice.p2p.commonapi.Endpoint;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.commonapi.Message;
import rice.p2p.commonapi.Node;
import rice.p2p.commonapi.NodeHandle;
import rice.p2p.commonapi.NodeHandleSet;
import rice.p2p.commonapi.RouteMessage;
import rice.p2p.commonapi.appsocket.AppSocket;
import rice.p2p.commonapi.appsocket.AppSocketReceiver;
import rice.p2p.commonapi.rawserialization.InputBuffer;
import rice.p2p.commonapi.rawserialization.MessageDeserializer;
import rice.p2p.past.*;
import rice.p2p.past.messaging.CacheMessage;
import rice.p2p.past.messaging.ContinuationMessage;
import rice.p2p.past.messaging.FetchHandleMessage;
import rice.p2p.past.messaging.FetchMessage;
import rice.p2p.past.messaging.InsertMessage;
import rice.p2p.past.messaging.LookupHandlesMessage;
import rice.p2p.past.messaging.LookupMessage;
import rice.p2p.past.messaging.MessageLostMessage;
import rice.p2p.past.messaging.PastMessage;
import rice.p2p.past.rawserialization.DefaultSocketStrategy;
import rice.p2p.past.rawserialization.JavaPastContentDeserializer;
import rice.p2p.past.rawserialization.JavaPastContentHandleDeserializer;
import rice.p2p.past.rawserialization.PastContentDeserializer;
import rice.p2p.past.rawserialization.PastContentHandleDeserializer;
import rice.p2p.past.rawserialization.SocketStrategy;
import rice.p2p.replication.Replication;
import rice.p2p.replication.manager.ReplicationManager;
import rice.p2p.replication.manager.ReplicationManagerClient;
import rice.p2p.replication.manager.ReplicationManagerImpl;
import rice.p2p.util.MathUtils;
import rice.p2p.util.rawserialization.SimpleInputBuffer;
import rice.p2p.util.rawserialization.SimpleOutputBuffer;
import rice.persistence.Cache;
import rice.persistence.LockManager;
import rice.persistence.LockManagerImpl;
import rice.persistence.StorageManager;

public class NewPastImpl implements Past, Application, ReplicationManagerClient {
    public final int MESSAGE_TIMEOUT;
    public final double SUCCESSFUL_INSERT_THRESHOLD;
    protected Endpoint endpoint;
    public StorageManager storage;
    protected StorageManager trash;
    protected Cache backup;
    protected int replicationFactor;
    protected ReplicationManager replicaManager;
    protected LockManager lockManager;
    protected PastPolicy policy;
    private int id;
    private Hashtable outstanding;
    private Hashtable timers;
    protected IdFactory factory;
    protected String instance;
    public int inserts;
    public int lookups;
    public int fetchHandles;
    public int other;
    protected Environment environment;
    protected Logger logger;
    protected PastContentDeserializer contentDeserializer;
    protected PastContentHandleDeserializer contentHandleDeserializer;
    public SocketStrategy socketStrategy;
    WeakHashMap pendingSocketTransactions;

    public NewPastImpl(Node node, StorageManager manager, int replicas, String instance) {
        this(node, manager, replicas, instance, new PastPolicy.DefaultPastPolicy());
    }

    public NewPastImpl(Node node, StorageManager manager, int replicas, String instance, PastPolicy policy) {
        this(node, manager, (Cache)null, replicas, instance, policy, (StorageManager)null);
    }

    public NewPastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash) {
        this(node, manager, backup, replicas, instance, policy, trash, false);
    }

    public NewPastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, boolean useOwnSocket) {
        this(node, manager, backup, replicas, instance, policy, trash, new DefaultSocketStrategy(useOwnSocket));
    }

    public NewPastImpl(Node node, StorageManager manager, Cache backup, int replicas, String instance, PastPolicy policy, StorageManager trash, SocketStrategy strategy) {
        this.inserts = 0;
        this.lookups = 0;
        this.fetchHandles = 0;
        this.other = 0;
        this.pendingSocketTransactions = new WeakHashMap();
        this.environment = node.getEnvironment();
        this.logger = this.environment.getLogManager().getLogger(this.getClass(), instance);
        this.logger.level = 0;
        Parameters p = this.environment.getParameters();
        this.MESSAGE_TIMEOUT = p.getInt("p2p_past_messageTimeout");
        this.SUCCESSFUL_INSERT_THRESHOLD = p.getDouble("p2p_past_successfulInsertThreshold");
        this.socketStrategy = strategy;
        this.storage = manager;
        this.backup = backup;
        this.contentDeserializer = new JavaPastContentDeserializer();
        this.contentHandleDeserializer = new JavaPastContentHandleDeserializer();
        this.endpoint = node.buildEndpoint(this, instance);
        this.endpoint.setDeserializer(new PastDeserializer());
        this.factory = node.getIdFactory();
        this.policy = policy;
        this.instance = instance;
        this.trash = trash;
        this.id = Integer.MIN_VALUE;
        this.outstanding = new Hashtable();
        this.timers = new Hashtable();
        this.replicationFactor = replicas;
        this.replicaManager = this.buildReplicationManager(node, instance);
        this.lockManager = new LockManagerImpl(this.environment);
        this.endpoint.accept(new AppSocketReceiver() {
            public void receiveSocket(AppSocket socket) {
                if (NewPastImpl.this.logger.level <= 500) {
//                    System.out.println("Received Socket from " + socket);
                }

                socket.register(true, false, 10000, this);
                NewPastImpl.this.endpoint.accept(this);
            }

            public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {
                if (NewPastImpl.this.logger.level <= 400) {
//                    System.out.println("Reading from " + socket);
                }

                try {
                    ByteBuffer[] bb = (ByteBuffer[])((ByteBuffer[])NewPastImpl.this.pendingSocketTransactions.get(socket));
                    if (bb == null) {
                        bb = new ByteBuffer[]{ByteBuffer.allocate(4)};
                        if (socket.read(bb, 0, 1) == -1L) {
                            this.close(socket);
                            return;
                        }

                        byte[] sizeArr = bb[0].array();
                        int size = MathUtils.byteArrayToInt(sizeArr);
                        if (NewPastImpl.this.logger.level <= 400) {
//                            System.out.println("Found object of size " + size + " from " + socket);
                        }

                        bb[0] = ByteBuffer.allocate(size);
                        NewPastImpl.this.pendingSocketTransactions.put(socket, bb);
                    }

                    if (socket.read(bb, 0, 1) == -1L) {
                        this.close(socket);
                    }

                    if (bb[0].remaining() == 0) {
                        NewPastImpl.this.pendingSocketTransactions.remove(socket);
                        if (NewPastImpl.this.logger.level <= 300) {
//                            System.out.println("bb[0].limit() " + bb[0].limit() + " bb[0].remaining() " + bb[0].remaining() + " from " + socket);
                        }

                        SimpleInputBuffer sib = new SimpleInputBuffer(bb[0].array());
                        short type = sib.readShort();
                        PastMessage result = (PastMessage)NewPastImpl.this.endpoint.getDeserializer().deserialize(sib, type, 0, (NodeHandle)null);
                        NewPastImpl.this.deliver((Id)null, result);
                    }

                    socket.register(true, false, 10000, this);
                } catch (IOException var8) {
                    this.receiveException(socket, var8);
                }

            }

            public void receiveException(AppSocket socket, Exception e) {
                if (NewPastImpl.this.logger.level <= 900) {
//                    System.out.println("Error receiving message" + e);
                }

                this.close(socket);
            }

            public void close(AppSocket socket) {
                if (socket != null) {
                    NewPastImpl.this.pendingSocketTransactions.remove(socket);
                    socket.close();
                }
            }
        });
        this.endpoint.register();
    }

    public String toString() {
        return this.endpoint == null ? super.toString() : "NewPastImpl[" + this.endpoint.getInstance() + "]";
    }

    public Environment getEnvironment() {
        return this.environment;
    }

    protected ReplicationManager buildReplicationManager(Node node, String instance) {
        return new ReplicationManagerImpl(node, this, this.replicationFactor, instance);
    }

    public Continuation[] getOutstandingMessages() {
        return (Continuation[])((Continuation[])this.outstanding.values().toArray(new Continuation[0]));
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    protected synchronized int getUID() {
        return this.id++;
    }

    protected Continuation getResponseContinuation(final PastMessage msg) {
        if (this.logger.level <= 400) {
//            System.out.println("Getting the Continuation to respond to the message " + msg + " and node handle " + this.getLocalNodeHandle());
        }

        final ContinuationMessage cmsg = (ContinuationMessage)msg;
        return new Continuation() {
            public void receiveResult(Object o) {
                cmsg.receiveResult(o);
                NewPastImpl.this.endpoint.route((Id)null, cmsg, msg.getSource());
            }

            public void receiveException(Exception e) {
                cmsg.receiveException(e);
                NewPastImpl.this.endpoint.route((Id)null, cmsg, msg.getSource());
            }
        };
    }

    protected Continuation getFetchResponseContinuation(final PastMessage msg) {
        final ContinuationMessage cmsg = (ContinuationMessage)msg;
        return new Continuation() {
            public void receiveResult(Object o) {
                cmsg.receiveResult(o);
                PastContent content = (PastContent)o;
                if (NewPastImpl.this.socketStrategy.sendAlongSocket(2, content)) {
                    NewPastImpl.this.sendViaSocket(msg.getSource(), cmsg, (Continuation)null);
                } else {
                    NewPastImpl.this.endpoint.route((Id)null, cmsg, msg.getSource());
                }

            }

            public void receiveException(Exception e) {
                cmsg.receiveException(e);
                NewPastImpl.this.endpoint.route((Id)null, cmsg, msg.getSource());
            }
        };
    }

    private void sendViaSocket(final NodeHandle handle, final PastMessage m, final Continuation c) {
        if (c != null) {
            CancellableTask timer = this.endpoint.scheduleMessage(new MessageLostMessage(m.getUID(), this.getLocalNodeHandle(), (Id)null, m, handle), (long)this.MESSAGE_TIMEOUT);
            this.insertPending(m.getUID(), timer, c);
        }

        SimpleOutputBuffer sob = new SimpleOutputBuffer();

        try {
            sob.writeInt(0);
            sob.writeShort(m.getType());
            m.serialize(sob);
        } catch (IOException var8) {
            if (c != null) {
                c.receiveException(var8);
            }
        }

        int size = sob.getWritten() - 4;
        if (this.logger.level <= 400) {
//            System.out.println("Sending size of " + size + " to " + handle + " to send " + m);
        }

        byte[] bytes = sob.getBytes();
        MathUtils.intToByteArray(size, bytes, 0);
        final ByteBuffer[] bb = new ByteBuffer[]{ByteBuffer.wrap(bytes, 0, sob.getWritten())};
        if (this.logger.level <= 500) {
//            System.out.println("Opening socket to " + handle + " to send " + m);
        }

        this.endpoint.connect(handle, new AppSocketReceiver() {
            public void receiveSocket(AppSocket socket) {
                if (NewPastImpl.this.logger.level <= 400) {
//                    System.out.println("Opened socket to " + handle + ":" + socket + " to send " + m);
                }

                socket.register(false, true, 10000, this);
            }

            public void receiveSelectResult(AppSocket socket, boolean canRead, boolean canWrite) {
                if (NewPastImpl.this.logger.level <= 300) {
//                    System.out.println("Writing to " + handle + ":" + socket + " to send " + m);
                }

                try {
                    socket.write(bb, 0, 1);
                } catch (IOException var5) {
                    if (c != null) {
                        c.receiveException(var5);
                    } else if (NewPastImpl.this.logger.level <= 900) {
//                        System.out.println("Error sending " + m + var5);
                    }

                    return;
                }

                if (bb[0].remaining() > 0) {
                    socket.register(false, true, 10000, this);
                } else {
                    socket.close();
                }

            }

            public void receiveException(AppSocket socket, Exception e) {
                if (c != null) {
                    c.receiveException(e);
                }

            }
        }, 10000);
    }

    protected void sendRequest(Id id, PastMessage message, Continuation command) {
        this.sendRequest(id, message, (NodeHandle)null, command);
    }

    protected void sendRequest(NodeHandle handle, PastMessage message, Continuation command) {
        this.sendRequest((Id)null, message, handle, command);
    }

    protected void sendRequest(Id id, PastMessage message, NodeHandle hint, Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println("Sending request message " + message + " {" + message.getUID() + "} to id " + id + " via " + hint);
        }

        CancellableTask timer = this.endpoint.scheduleMessage(new MessageLostMessage(message.getUID(), this.getLocalNodeHandle(), id, message, hint), (long)this.MESSAGE_TIMEOUT);
        this.insertPending(message.getUID(), timer, command);
        this.endpoint.route(id, message, hint);
    }

    private void insertPending(int uid, CancellableTask timer, Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println("Loading continuation " + uid + " into pending table");
        }

        this.timers.put(uid, timer);
        this.outstanding.put(uid, command);
    }

    private Continuation removePending(int uid) {
        if (this.logger.level <= 400) {
//            System.out.println("Removing and returning continuation " + uid + " from pending table");
        }

        CancellableTask timer = (CancellableTask)this.timers.remove(uid);
        if (timer != null) {
            timer.cancel();
        }

        return (Continuation)this.outstanding.remove(uid);
    }

    private void handleResponse(PastMessage message) {
        if (this.logger.level <= 500) {
//            System.out.println("handling reponse message " + message + " from the request at node "+this.getLocalNodeHandle());
        }

        Continuation command = this.removePending(message.getUID());
        if (command != null) {
            message.returnResponse(command, this.environment, this.instance);
        }

    }

    protected void getHandles(Id id, final int max, Continuation command) {
        NodeHandleSet set = this.endpoint.replicaSet(id, max);
        if (set.size() == max) {
            command.receiveResult(set);
        } else {
            this.sendRequest((Id)id, new LookupHandlesMessage(this.getUID(), id, max, this.getLocalNodeHandle(), id), new Continuation.StandardContinuation(command) {
                public void receiveResult(Object o) {
                    NodeHandleSet replicas = (NodeHandleSet)o;
                    if (Math.min(max, NewPastImpl.this.endpoint.replicaSet(NewPastImpl.this.endpoint.getLocalNodeHandle().getId(), NewPastImpl.this.replicationFactor + 1).size()) > replicas.size()) {
                        this.parent.receiveException(new PastException("Only received " + replicas.size() + " replicas - cannot insert as we know about more nodes."));
                    } else {
                        this.parent.receiveResult(replicas);
                    }

                }
            });
        }

    }

    private void cache(PastContent content) {
        this.cache(content, new Continuation.ListenerContinuation("Caching of " + content, this.environment));
    }

    public void cache(PastContent content, Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println("Inserting PastContent object " + content + " into cache. This node "+this.getLocalNodeHandle());
        }

        if (content != null && !content.isMutable()) {
            this.storage.cache(content.getId(), (Serializable)null, content, command);
        } else {
            command.receiveResult(true);
        }

    }

    protected void doInsert(final Id id, final MessageBuilder builder, Continuation command, final boolean useSocket) {
        this.getHandles(id, this.replicationFactor + 1, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object o) {
                NodeHandleSet replicas = (NodeHandleSet)o;
                if (NewPastImpl.this.logger.level <= 400) {
//                    System.out.println("Received replicas " + replicas + " for id " + id);
                }

                Continuation.MultiContinuation multi = new Continuation.MultiContinuation(this.parent, replicas.size()) {
                    public boolean isDone() throws Exception {
                        int numSuccess = 0;

                        int i;
                        for(i = 0; i < this.haveResult.length; ++i) {
                            if (this.haveResult[i] && Boolean.TRUE.equals(this.result[i])) {
                                ++numSuccess;
                            }
                        }

                        if ((double)numSuccess >= NewPastImpl.this.SUCCESSFUL_INSERT_THRESHOLD * (double)this.haveResult.length) {
                            return true;
                        } else if (super.isDone()) {
                            for(i = 0; i < this.result.length; ++i) {
                                if (this.result[i] instanceof Exception && NewPastImpl.this.logger.level <= 900) {
//                                    System.out.println("result[" + i + "]:" + (Exception)this.result[i]);
                                }
                            }

                            throw new PastException("Had only " + numSuccess + " successful inserts out of " + this.result.length + " - aborting.");
                        } else {
                            return false;
                        }
                    }

                    public Object getResult() {
                        Boolean[] b = new Boolean[this.result.length];

                        for(int i = 0; i < b.length; ++i) {
                            b[i] = this.result[i] == null || Boolean.TRUE.equals(this.result[i]);
                        }

                        return b;
                    }
                };

                for(int i = 0; i < replicas.size(); ++i) {
                    NodeHandle handle = replicas.getHandle(i);
                    PastMessage m = builder.buildMessage();
                    Continuation c = new Continuation.NamedContinuation("InsertMessage to " + replicas.getHandle(i) + " for " + id, multi.getSubContinuation(i));
                    if (useSocket) {
                        NewPastImpl.this.sendViaSocket(handle, m, c);
                    } else {
                        NewPastImpl.this.sendRequest((NodeHandle)handle, m, c);
                    }
                }

            }
        });
    }

    public void insert(final PastContent obj, Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println("Inserting the object " + obj + " with the id " + obj.getId());
        }

        if (this.logger.level <= 300) {
//            System.out.println(" Inserting data of class " + obj.getClass().getName() + " under " + obj.getId().toStringFull());
        }

        this.doInsert(obj.getId(), new MessageBuilder() {
            public PastMessage buildMessage() {
                return new InsertMessage(NewPastImpl.this.getUID(), obj, NewPastImpl.this.getLocalNodeHandle(), obj.getId());
            }
        }, new Continuation.StandardContinuation(command) {
            public void receiveResult(final Object array) {
                NewPastImpl.this.cache(obj, new Continuation.SimpleContinuation() {
                    public void receiveResult(Object o) {
                        parent.receiveResult(array);
                    }
                });
            }
        }, this.socketStrategy.sendAlongSocket(1, obj));
    }

    public void lookup(Id id, Continuation<PastContent, Exception> command) {
        this.lookup(id, true, command);
    }

    public void lookup(final Id id, final boolean cache, final Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println(" Performing lookup on " + id.toStringFull());
        }

        this.storage.getObject(id, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object o) {
                if (o != null) {
                    command.receiveResult(o);
                } else {
                    NewPastImpl.this.sendRequest((Id)id, new LookupMessage(NewPastImpl.this.getUID(), id, NewPastImpl.this.getLocalNodeHandle(), id), new Continuation.NamedContinuation("LookupMessage for " + id, this) {
                        public void receiveResult(final Object o) {
                            if (o != null) {
                                if (cache) {
                                    NewPastImpl.this.cache((PastContent)o, new Continuation.SimpleContinuation() {
                                        public void receiveResult(Object object) {
                                            command.receiveResult(o);
                                        }
                                    });
                                } else {
                                    command.receiveResult(o);
                                }
                            } else {
                                NewPastImpl.this.lookupHandles(id, NewPastImpl.this.replicationFactor + 1, new Continuation() {
                                    public void receiveResult(Object o) {
                                        PastContentHandle[] handles = (PastContentHandle[])((PastContentHandle[])o);

                                        for(int i = 0; i < handles.length; ++i) {
                                            if (handles[i] != null) {
                                                NewPastImpl.this.fetch(handles[i], new Continuation.StandardContinuation(parent) {
                                                    public void receiveResult(final Object o) {
                                                        if (cache) {
                                                            NewPastImpl.this.cache((PastContent)o, new Continuation.SimpleContinuation() {
                                                                public void receiveResult(Object object) {
                                                                    command.receiveResult(o);
                                                                }
                                                            });
                                                        } else {
                                                            command.receiveResult(o);
                                                        }

                                                    }
                                                });
                                                return;
                                            }
                                        }

                                        command.receiveResult((Object)null);
                                    }

                                    public void receiveException(Exception e) {
                                        command.receiveException(e);
                                    }
                                });
                            }

                        }

                        public void receiveException(Exception e) {
                            this.receiveResult((Object)null);
                        }
                    });
                }

            }
        });
    }

    public void lookupHandles(final Id id, int max, Continuation command) {
        if (this.logger.level <= 500) {
//            System.out.println("Retrieving handles of up to " + max + " replicas of the object stored in Past with id " + id);
        }

        if (this.logger.level <= 400) {
//            System.out.println("Fetching up to " + max + " handles of " + id.toStringFull());
        }

        this.getHandles(id, max, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object o) {
                NodeHandleSet replicas = (NodeHandleSet)o;
                if (NewPastImpl.this.logger.level <= 400) {
//                    System.out.println("Receiving replicas " + replicas + " for lookup Id " + id);
                }

                Continuation.MultiContinuation multi = new Continuation.MultiContinuation(this.parent, replicas.size()) {
                    public Object getResult() {
                        PastContentHandle[] p = new PastContentHandle[this.result.length];

                        for(int i = 0; i < this.result.length; ++i) {
                            if (this.result[i] instanceof PastContentHandle) {
                                p[i] = (PastContentHandle)this.result[i];
                            }
                        }

                        return p;
                    }
                };

                for(int i = 0; i < replicas.size(); ++i) {
                    NewPastImpl.this.lookupHandle(id, replicas.getHandle(i), multi.getSubContinuation(i));
                }

            }
        });
    }

    public void lookupHandle(Id id, NodeHandle handle, Continuation command) {
        if (this.logger.level <= 500) {
//            System.out.println("Retrieving handle for id " + id + " from node " + handle);
        }

        this.sendRequest((NodeHandle)handle, new FetchHandleMessage(this.getUID(), id, this.getLocalNodeHandle(), handle.getId()), new Continuation.NamedContinuation("FetchHandleMessage to " + handle + " for " + id, command));
    }

    public void fetch(PastContentHandle handle, Continuation command) {
        if (this.logger.level <= 500) {
//            System.out.println("Retrieving object associated with content handle " + handle);
        }

        if (this.logger.level <= 400) {
//            System.out.println("Fetching object under id " + handle.getId().toStringFull() + " on " + handle.getNodeHandle());
        }

        NodeHandle han = handle.getNodeHandle();
        this.sendRequest((NodeHandle)han, new FetchMessage(this.getUID(), handle, this.getLocalNodeHandle(), han.getId()), new Continuation.NamedContinuation("FetchMessage to " + handle.getNodeHandle() + " for " + handle.getId(), command));
    }

    public NodeHandle getLocalNodeHandle() {
        return this.endpoint.getLocalNodeHandle();
    }

    public int getReplicationFactor() {
        return this.replicationFactor;
    }

    public boolean forward(RouteMessage message) {
        Message internal;
        try {
            internal = message.getMessage(this.endpoint.getDeserializer());
        } catch (IOException var5) {
            throw new RuntimeException(var5);
        }

        if (internal instanceof LookupMessage) {
            LookupMessage lmsg = (LookupMessage)internal;
            Id id = lmsg.getId();
            if (!lmsg.isResponse()) {
                if (this.logger.level <= 400) {
//                    System.out.println("Lookup message " + lmsg + " is a request; look in the cache . This node is "+this.getLocalNodeHandle());
                }

                if (this.storage.exists(id)) {
                    if (this.logger.level <= 500) {
//                        System.out.println("Request for " + id + " satisfied locally - responding. Endpoint is"+this.endpoint.getId().toStringFull() + " nodehandle is "+this.getLocalNodeHandle());
                    }

                    this.deliver(this.endpoint.getId(), lmsg);
                    return false;
                }
            }
        } else if (internal instanceof LookupHandlesMessage) {
            LookupHandlesMessage lmsg = (LookupHandlesMessage)internal;
            if (!lmsg.isResponse() && this.endpoint.replicaSet(lmsg.getId(), lmsg.getMax()).size() == lmsg.getMax()) {
                if (this.logger.level <= 500) {
//                    System.out.println("Hijacking lookup handles request for " + lmsg.getId());
                }

                this.deliver(this.endpoint.getId(), lmsg);
                return false;
            }
        }

        return true;
    }

    public void deliver(Id id, Message message) {
        final PastMessage msg = (PastMessage)message;
        if (msg.isResponse()) {
            this.handleResponse((PastMessage)message);
        } else {
            if (this.logger.level <= 800) {
//                System.out.println("Received message " + message + " with destination " + id +" currently in "+this.getLocalNodeHandle() );
            }

            if (msg instanceof InsertMessage) {
                final InsertMessage imsg = (InsertMessage)msg;
                if (this.policy.allowInsert(imsg.getContent())) {
                    ++this.inserts;
                    final Id msgid = imsg.getContent().getId();
                    this.lockManager.lock(msgid, new Continuation.StandardContinuation(this.getResponseContinuation(msg)) {
                        public void receiveResult(Object result) {
                            NewPastImpl.this.storage.getObject(msgid, new Continuation.StandardContinuation(this.parent) {
                                public void receiveResult(Object o) {
                                    try {
                                        PastContent content;
                                        if(o != null){
                                            content = imsg.getContent();
                                        }
                                        else{
                                            content = imsg.getContent().checkInsert(msgid, (PastContent)o);
                                        }
                                        NewPastImpl.this.storage.store(msgid, (Serializable)null, content, new Continuation.StandardContinuation(this.parent) {
                                            public void receiveResult(Object result) {
                                                NewPastImpl.this.getResponseContinuation(msg).receiveResult(result);
                                                NewPastImpl.this.lockManager.unlock(msgid);
                                            }
                                        });
                                    } catch (PastException var3) {
                                        this.parent.receiveException(var3);
                                    }

                                }
                            });
                        }
                    });
                } else {
                    this.getResponseContinuation(msg).receiveResult(false);
                }
            } else if (msg instanceof LookupMessage) {
                final LookupMessage lmsg = (LookupMessage)msg;
                ++this.lookups;
                this.storage.getObject(lmsg.getId(), new Continuation.StandardContinuation(this.getResponseContinuation(lmsg)) {
                    public void receiveResult(Object o) {
                        if (NewPastImpl.this.logger.level <= 500) {
//                            System.out.println("[Continuation]Received object " + o + " for id " + lmsg.getId());
                        }

                        this.parent.receiveResult(o);
                        if (lmsg.getPreviousNodeHandle() != null && o != null && !((PastContent)o).isMutable()) {
                            NodeHandle handle = lmsg.getPreviousNodeHandle();
                            if (NewPastImpl.this.logger.level <= 500) {
//                                System.out.println("[Continuation]Pushing cached copy of " + ((PastContent)o).getId() + " to " + handle);
                            }

                            new CacheMessage(NewPastImpl.this.getUID(), (PastContent)o, NewPastImpl.this.getLocalNodeHandle(), handle.getId());
                        }

                    }
                });
            } else if (msg instanceof LookupHandlesMessage) {
                LookupHandlesMessage lmsg = (LookupHandlesMessage)msg;
                NodeHandleSet set = this.endpoint.replicaSet(lmsg.getId(), lmsg.getMax());
                if (this.logger.level <= 400) {
//                    System.out.println("Returning replica set " + set + " for lookup handles of id " + lmsg.getId() + " max " + lmsg.getMax() + " at " + this.endpoint.getId());
                }

                this.getResponseContinuation(msg).receiveResult(set);
            } else if (msg instanceof FetchMessage) {
                FetchMessage fmsg = (FetchMessage)msg;
                ++this.lookups;
                Continuation c = this.getFetchResponseContinuation(msg);
                this.storage.getObject(fmsg.getHandle().getId(), c);
            } else if (msg instanceof FetchHandleMessage) {
                final FetchHandleMessage fmsg = (FetchHandleMessage)msg;
                ++this.fetchHandles;
                this.storage.getObject(fmsg.getId(), new Continuation.StandardContinuation(this.getResponseContinuation(msg)) {
                    public void receiveResult(Object o) {
                        PastContent content = (PastContent)o;
                        if (content != null) {
                            if (NewPastImpl.this.logger.level <= 500) {
//                                System.out.println("[Continuation]Retrieved data for fetch handles of id " + fmsg.getId());
                            }

                            this.parent.receiveResult(content.getHandle(NewPastImpl.this));
                        } else {
                            this.parent.receiveResult((Object)null);
                        }

                    }
                });
            } else if (msg instanceof CacheMessage) {
                this.cache(((CacheMessage)msg).getContent());
            } else if (this.logger.level <= 1000) {
//                System.out.println("ERROR - Received message " + msg + "of unknown type.");
            }
        }

    }

    public void update(NodeHandle handle, boolean joined) {
    }

    public void fetch(final Id id, NodeHandle hint, Continuation command) {
        if (this.logger.level <= 400) {
//            System.out.println("Sending out replication fetch request for the id " + id);
        }

        this.policy.fetch(id, hint, this.backup, this, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object o) {
                if (o == null) {
                    if (NewPastImpl.this.logger.level <= 900) {
//                        System.out.println("Could not fetch id " + id + " - policy returned null in namespace " + NewPastImpl.this.instance);
                    }

                    this.parent.receiveResult(false);
                } else {
                    if (NewPastImpl.this.logger.level <= 300) {
//                        System.out.println("inserting replica of id " + id);
                    }

                    if (!(o instanceof PastContent) && NewPastImpl.this.logger.level <= 900) {
//                        System.out.println("ERROR! Not PastContent " + o.getClass().getName() + " " + o);
                    }

                    NewPastImpl.this.storage.getStorage().store(((PastContent)o).getId(), (Serializable)null, (PastContent)o, this.parent);
                }

            }
        });
    }

    public void remove(final Id id, Continuation command) {
        if (this.backup != null) {
            this.storage.getObject(id, new Continuation.StandardContinuation(command) {
                public void receiveResult(Object o) {
                    NewPastImpl.this.backup.cache(id, NewPastImpl.this.storage.getMetadata(id), (Serializable)o, new Continuation.StandardContinuation(this.parent) {
                        public void receiveResult(Object o) {
                            NewPastImpl.this.storage.unstore(id, this.parent);
                        }
                    });
                }
            });
        } else {
            this.storage.unstore(id, command);
        }

    }

    public IdSet scan(IdRange range) {
        return this.storage.getStorage().scan(range);
    }

    public IdSet scan() {
        return this.storage.getStorage().scan();
    }

    public boolean exists(Id id) {
        return this.storage.getStorage().exists(id);
    }

    public void existsInOverlay(Id id, Continuation command) {
        this.lookupHandles(id, this.replicationFactor + 1, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object result) {
                Object[] results = (Object[])((Object[])result);

                for(int i = 0; i < results.length; ++i) {
                    if (results[i] instanceof PastContentHandle) {
                        this.parent.receiveResult(Boolean.TRUE);
                        return;
                    }
                }

                this.parent.receiveResult(Boolean.FALSE);
            }
        });
    }

    public void reInsert(Id id, Continuation command) {
        this.storage.getObject(id, new Continuation.StandardContinuation(command) {
            public void receiveResult(Object o) {
                NewPastImpl.this.insert((PastContent)o, new Continuation.StandardContinuation(this.parent) {
                    public void receiveResult(Object result) {
                        Boolean[] results = (Boolean[])((Boolean[])result);

                        for(int i = 0; i < results.length; ++i) {
                            if (results[i]) {
                                this.parent.receiveResult(Boolean.TRUE);
                                return;
                            }
                        }

                        this.parent.receiveResult(Boolean.FALSE);
                    }
                });
            }
        });
    }

    public Replication getReplication() {
        return this.replicaManager.getReplication();
    }

    public StorageManager getStorageManager() {
        return this.storage;
    }

    public String getInstance() {
        return this.instance;
    }

    public void setContentDeserializer(PastContentDeserializer deserializer) {
        this.contentDeserializer = deserializer;
    }

    public void setContentHandleDeserializer(PastContentHandleDeserializer deserializer) {
        this.contentHandleDeserializer = deserializer;
    }

    public interface MessageBuilder {
        PastMessage buildMessage();
    }

    protected class PastDeserializer implements MessageDeserializer {
        protected PastDeserializer() {
        }

        public Message deserialize(InputBuffer buf, short type, int priority, NodeHandle sender) throws IOException {
            try {
                switch (type) {
                    case 1:
                        return CacheMessage.build(buf, NewPastImpl.this.endpoint, NewPastImpl.this.contentDeserializer);
                    case 2:
                        return FetchHandleMessage.build(buf, NewPastImpl.this.endpoint, NewPastImpl.this.contentHandleDeserializer);
                    case 3:
                        return FetchMessage.build(buf, NewPastImpl.this.endpoint, NewPastImpl.this.contentDeserializer, NewPastImpl.this.contentHandleDeserializer);
                    case 4:
                        return InsertMessage.build(buf, NewPastImpl.this.endpoint, NewPastImpl.this.contentDeserializer);
                    case 5:
                        return LookupHandlesMessage.build(buf, NewPastImpl.this.endpoint);
                    case 6:
                        return LookupMessage.build(buf, NewPastImpl.this.endpoint, NewPastImpl.this.contentDeserializer);
                }
            } catch (IOException var6) {
                if (NewPastImpl.this.logger.level <= 1000) {
//                    System.out.println("Exception in deserializer in " + NewPastImpl.this.endpoint.toString() + ":" + NewPastImpl.this.instance + " " + var6);
                }

                throw var6;
            }

            throw new IllegalArgumentException("Unknown type:" + type + " in " + NewPastImpl.this.toString());
        }
    }
}
