package com.example.pastry.past;

//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.Vector;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import rice.Continuation;
import rice.environment.Environment;
import rice.environment.logging.Logger;
import rice.environment.processing.WorkRequest;
import rice.p2p.commonapi.Id;
import rice.p2p.commonapi.IdFactory;
import rice.p2p.commonapi.IdRange;
import rice.p2p.commonapi.IdSet;
import rice.p2p.util.ImmutableSortedMap;
import rice.p2p.util.RedBlackMap;
import rice.p2p.util.ReverseTreeMap;
import rice.p2p.util.XMLObjectInputStream;
import rice.p2p.util.XMLObjectOutputStream;
import rice.p2p.util.rawserialization.JavaSerializationException;
import rice.persistence.Storage;
import rice.selector.Timer;
import rice.selector.TimerTask;

public class NewPersistentStorage implements Storage {
    private Object statLock;
    private long statsLastWritten;
    private long statsWriteInterval;
    private long numWrites;
    private long numReads;
    private long numRenames;
    private long numDeletes;
    private long numMetadataWrites;
    public static final long PERSISTENCE_MAGIC_NUMBER = 8038844221L;
    public static final long PERSISTENCE_VERSION_2 = 2L;
    public static final long PERSISTENCE_REVISION_2_0 = 0L;
    public static final long PERSISTENCE_REVISION_2_1 = 1L;
    public static final String BACKUP_DIRECTORY = "/FreePastry-Storage-Root/";
    public static final String LOST_AND_FOUND_DIRECTORY = "lost+found";
    public static final String METADATA_FILENAME = "metadata.cache";
    public static final int MAX_FILES = 256;
    public static final int MAX_DIRECTORIES = 32;
    public static final int METADATA_SYNC_TIME = 300000;
    public static final String ZERO_LENGTH_NAME = "!";
    private IdFactory factory;
    private String name;
    private File rootDirectory;
    private File backupDirectory;
    private File appDirectory;
    private File lostDirectory;
    private boolean index;
    private HashMap directories;
    private HashMap prefixes;
    private HashSet dirty;
    private ReverseTreeMap metadata;
    private String rootDir;
    private long storageSize;
    private long usedSize;
    Environment environment;
    Logger logger;

    public NewPersistentStorage(IdFactory factory, String rootDir, long size, Environment env) throws IOException {
        this(factory, "default", rootDir, size, env);
    }

    public NewPersistentStorage(IdFactory factory, String name, String rootDir, long size, Environment env) throws IOException {
        this(factory, name, rootDir, size, true, env);
    }

    public NewPersistentStorage(IdFactory factory, String name, String rootDir, long size, boolean index, Environment env) throws IOException {
        this.statLock = new Object();
        this.statsWriteInterval = 60000L;
        this.numWrites = 0L;
        this.numReads = 0L;
        this.numRenames = 0L;
        this.numDeletes = 0L;
        this.numMetadataWrites = 0L;
        this.environment = env;
        this.logger = this.environment.getLogManager().getLogger(NewPersistentStorage.class, (String)null);
        this.factory = factory;
        this.name = name;
        this.rootDir = rootDir;
        this.storageSize = size;
        this.index = index;
        this.directories = new HashMap();
        this.prefixes = new HashMap();
        this.statsLastWritten = this.environment.getTimeSource().currentTimeMillis();
        if (index) {
            this.dirty = new HashSet();
            this.metadata = new ReverseTreeMap();
        }

        if (this.logger.level <= 800) {
            this.logger.log("Launching persistent storage in " + rootDir + " with name " + name + " spliting factor " + 256);
        }

        this.init();
    }

    private void printStats() {
        synchronized(this.statLock) {
            long now = this.environment.getTimeSource().currentTimeMillis();
            if (this.statsLastWritten / this.statsWriteInterval != now / this.statsWriteInterval) {
                if (this.logger.level <= 800) {
                    this.logger.log("@L.PE name=" + this.name + " interval=" + this.statsLastWritten + "-" + now);
                }

                this.statsLastWritten = now;
                if (this.logger.level <= 800) {
                    this.logger.log("@L.PE   objsTotal=" + (this.index ? "" + this.metadata.keySet().size() : "?") + " objsBytesTotal=" + this.getTotalSize());
                }

                if (this.logger.level <= 800) {
                    this.logger.log("@L.PE   numWrites=" + this.numWrites + " numReads=" + this.numReads + " numDeletes=" + this.numDeletes);
                }

                if (this.logger.level <= 800) {
                    this.logger.log("@L.PE   numMetadataWrites=" + this.numMetadataWrites + " numRenames=" + this.numRenames);
                }
            }

        }
    }

    public void setTimer(Timer timer) {
        if (this.index) {
            timer.scheduleAtFixedRate(new TimerTask() {
                public String toString() {
                    return "persistence dirty purge enqueue";
                }

                public void run() {
                    NewPersistentStorage.this.environment.getProcessor().processBlockingIO(new WorkRequest(new Continuation.ListenerContinuation("Enqueue of writeMetadataFile", NewPersistentStorage.this.environment), NewPersistentStorage.this.environment.getSelectorManager()) {
                        public String toString() {
                            return "persistence dirty purge";
                        }

                        public Object doWork() throws Exception {
                            NewPersistentStorage.this.writeDirty();
                            return Boolean.TRUE;
                        }
                    });
                }
            }, (long)this.environment.getRandomSource().nextInt(300000), 300000L);
        }

    }

    public void rename(final Id oldId, final Id newId, Continuation c) {
        this.printStats();
        this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
            public String toString() {
                return "rename " + oldId + " " + newId;
            }

            public Object doWork() throws Exception {
                synchronized(NewPersistentStorage.this.statLock) {
                    NewPersistentStorage.this.numRenames++;
                }

                File f = NewPersistentStorage.this.getFile(oldId);
                if (f != null && f.exists()) {
                    File g = NewPersistentStorage.this.getFile(newId);
                    NewPersistentStorage.renameFile(f, g);
                    NewPersistentStorage.this.checkDirectory(g.getParentFile());
                    if (NewPersistentStorage.this.index) {
                        synchronized(NewPersistentStorage.this.metadata) {
                            NewPersistentStorage.this.metadata.put(newId, NewPersistentStorage.this.metadata.get(oldId));
                            NewPersistentStorage.this.metadata.remove(oldId);
                        }
                    }

                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            }
        });
    }

    public void store(final Id id, final Serializable metadata, final Serializable obj, Continuation c) {
        if (id != null && obj != null) {
            this.printStats();
            this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
                public String toString() {
                    return "store " + id;
                }

                public Object doWork() throws Exception {
                    synchronized(NewPersistentStorage.this.statLock) {
                        NewPersistentStorage.this.numWrites++;
                    }

                    if (NewPersistentStorage.this.logger.level <= 400) {
                        NewPersistentStorage.this.logger.log("Storing object " + obj + " under id " + id.toStringFull() + " in root " + NewPersistentStorage.this.appDirectory);
                    }

                    File objFile = NewPersistentStorage.this.getFile(id);
                    File transcFile = NewPersistentStorage.this.makeTemporaryFile(id);
                    if (NewPersistentStorage.this.logger.level <= 400) {
                        NewPersistentStorage.this.logger.log("Writing object " + obj + " to temporary file " + transcFile + " and renaming to " + objFile);
                    }

                    try {
                        NewPersistentStorage.writeObject(obj, metadata, id, NewPersistentStorage.this.environment.getTimeSource().currentTimeMillis(), transcFile);
                        if (NewPersistentStorage.this.logger.level <= 400) {
                            NewPersistentStorage.this.logger.log("Done writing object " + obj + " under id " + id.toStringFull() + " in root " + NewPersistentStorage.this.appDirectory);
                        }

                        if (NewPersistentStorage.this.getUsedSpace() + NewPersistentStorage.getFileLength(transcFile) > NewPersistentStorage.this.getStorageSize()) {
                            throw new NewPersistentStorage.OutofDiskSpaceException();
                        }
                    } catch (Exception var7) {
                        if (NewPersistentStorage.this.logger.level <= 900) {
                            NewPersistentStorage.this.logger.logException("", var7);
                        }

                        NewPersistentStorage.deleteFile(transcFile);
                        throw var7;
                    }

                    if (NewPersistentStorage.this.logger.level <= 400) {
                        NewPersistentStorage.this.logger.log("COUNT: Storing data of class " + obj.getClass().getName() + " under " + id.toStringFull() + " of size " + transcFile.length() + " in " + NewPersistentStorage.this.name);
                    }

                    NewPersistentStorage.this.decreaseUsedSpace(NewPersistentStorage.getFileLength(objFile));
                    NewPersistentStorage.this.increaseUsedSpace(NewPersistentStorage.getFileLength(transcFile));
                    NewPersistentStorage.renameFile(transcFile, objFile);
                    if (NewPersistentStorage.this.index) {
                        synchronized(NewPersistentStorage.this.metadata) {
                            NewPersistentStorage.this.metadata.put(id, metadata);
                            NewPersistentStorage.this.dirty.add(objFile.getParentFile());
                        }
                    }

                    NewPersistentStorage.this.checkDirectory(objFile.getParentFile());
                    return Boolean.TRUE;
                }
            });
        } else {
            c.receiveResult(false);
        }
    }

    public void unstore(final Id id, Continuation c) {
        this.printStats();
        this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
            public String toString() {
                return "unstore " + id;
            }

            public Object doWork() throws Exception {
                synchronized(NewPersistentStorage.this.statLock) {
                    NewPersistentStorage.this.numDeletes++;
                }

                File objFile = NewPersistentStorage.this.getFile(id);
                if (NewPersistentStorage.this.logger.level <= 400) {
                    NewPersistentStorage.this.logger.log("COUNT: Unstoring data under " + id.toStringFull() + " of size " + objFile.length() + " in " + NewPersistentStorage.this.name);
                }

                if (NewPersistentStorage.this.index) {
                    synchronized(NewPersistentStorage.this.metadata) {
                        NewPersistentStorage.this.metadata.remove(id);
                        NewPersistentStorage.this.dirty.add(objFile.getParentFile());
                    }
                }

                if (objFile != null && objFile.exists()) {
                    NewPersistentStorage.this.decreaseUsedSpace(objFile.length());
                    NewPersistentStorage.deleteFile(objFile);
                    return Boolean.TRUE;
                } else {
                    return Boolean.FALSE;
                }
            }
        });
    }

    public boolean exists(Id id) {
        if (this.index) {
            synchronized(this.metadata) {
                return this.metadata.containsKey(id);
            }
        } else {
            throw new UnsupportedOperationException("exists() not supported without indexing");
        }
    }

    public Serializable getMetadata(Id id) {
        if (this.index) {
            synchronized(this.metadata) {
                return (Serializable)this.metadata.get(id);
            }
        } else {
            throw new UnsupportedOperationException("getMetadata() not supported without indexing");
        }
    }

    public void setMetadata(final Id id, final Serializable metadata, Continuation c) {
        this.printStats();
        if (!this.exists(id)) {
            c.receiveResult(false);
        } else {
            this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
                public String toString() {
                    return "setMetadata " + id;
                }

                public Object doWork() throws Exception {
                    synchronized(NewPersistentStorage.this.statLock) {
                        NewPersistentStorage.this.numMetadataWrites++;
                    }

                    if (NewPersistentStorage.this.logger.level <= 400) {
                        NewPersistentStorage.this.logger.log("COUNT: Updating metadata for " + id.toStringFull() + " in " + NewPersistentStorage.this.name);
                    }

                    File objFile = NewPersistentStorage.this.getFile(id);
                    NewPersistentStorage.writeMetadata(objFile, metadata);
                    if (NewPersistentStorage.this.index) {
                        synchronized(NewPersistentStorage.this.metadata) {
                            NewPersistentStorage.this.metadata.put(id, metadata);
                            NewPersistentStorage.this.dirty.add(objFile.getParentFile());
                        }
                    }

                    return Boolean.TRUE;
                }
            });
        }

    }

    public void getObject(final Id id, Continuation c) {
        this.printStats();
        if (this.index && !this.exists(id)) {
            c.receiveResult((Object)null);
        } else {
            this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
                public String toString() {
                    return "getObject " + id;
                }

                public Object doWork() throws Exception {
                    synchronized(NewPersistentStorage.this.statLock) {
                        NewPersistentStorage.this.numReads++;
                    }

                    File objFile = NewPersistentStorage.this.getFile(id);

                    try {
                        if (objFile != null && objFile.exists()) {
                            if (NewPersistentStorage.this.logger.level <= 400) {
                                NewPersistentStorage.this.logger.log("COUNT: Fetching data under " + id.toStringFull() + " of size " + objFile.length() + " in " + NewPersistentStorage.this.name);
                            }

                            return NewPersistentStorage.readData(objFile);
                        } else {
                            return null;
                        }
                    } catch (Exception var7) {
                        if (NewPersistentStorage.this.index) {
                            synchronized(NewPersistentStorage.this.metadata) {
                                NewPersistentStorage.this.metadata.remove(id);
                                NewPersistentStorage.this.dirty.add(objFile.getParentFile());
                            }
                        }

                        NewPersistentStorage.this.moveToLost(objFile);
                        throw var7;
                    }
                }
            });
        }

    }

    public IdSet scan(IdRange range) {
        if (this.index) {
            if (range.isEmpty()) {
                return this.factory.buildIdSet();
            } else if (range.getCCWId().equals(range.getCWId())) {
                return this.scan();
            } else {
                synchronized(this.metadata) {
                    return this.factory.buildIdSet(new ImmutableSortedMap(this.metadata.keySubMap(range.getCCWId(), range.getCWId())));
                }
            }
        } else {
            throw new UnsupportedOperationException("scan() not supported without indexing");
        }
    }

    public IdSet scan() {
        if (this.index) {
            synchronized(this.metadata) {
                return this.factory.buildIdSet(new ImmutableSortedMap(this.metadata.keyMap()));
            }
        } else {
            throw new UnsupportedOperationException("scan() not supported without indexing");
        }
    }

    public SortedMap scanMetadata(IdRange range) {
        if (this.index) {
            if (range.isEmpty()) {
                return new RedBlackMap();
            } else if (range.getCCWId().equals(range.getCWId())) {
                return this.scanMetadata();
            } else {
                synchronized(this.metadata) {
                    return new ImmutableSortedMap(this.metadata.keySubMap(range.getCCWId(), range.getCWId()));
                }
            }
        } else {
            throw new UnsupportedOperationException("scanMetadata() not supported without indexing");
        }
    }

    public SortedMap scanMetadata() {
        if (this.index) {
            return new ImmutableSortedMap(this.metadata.keyMap());
        } else {
            throw new UnsupportedOperationException("scanMetadata() not supported without indexing");
        }
    }

    public SortedMap scanMetadataValuesHead(Object value) {
        if (this.index) {
            return new ImmutableSortedMap(this.metadata.valueHeadMap(value));
        } else {
            throw new UnsupportedOperationException("scanMetadataValuesHead() not supported without indexing");
        }
    }

    public SortedMap scanMetadataValuesNull() {
        if (this.index) {
            return new ImmutableSortedMap(this.metadata.valueNullMap());
        } else {
            throw new UnsupportedOperationException("scanMetadataValuesNull() not supported without indexing");
        }
    }

    public long getTotalSize() {
        return this.usedSize;
    }

    public int getSize() {
        if (this.index) {
            return this.metadata.size();
        } else {
            throw new UnsupportedOperationException("getSize() not supported without indexing");
        }
    }

    public void flush(Continuation c) {
        this.environment.getProcessor().processBlockingIO(new WorkRequest(c, this.environment.getSelectorManager()) {
            public String toString() {
                return "flush";
            }

            public Object doWork() throws Exception {
                if (NewPersistentStorage.this.logger.level <= 400) {
                    NewPersistentStorage.this.logger.log("COUNT: Flushing all data in " + NewPersistentStorage.this.name);
                }

                NewPersistentStorage.this.flushDirectory(NewPersistentStorage.this.appDirectory);
                return Boolean.TRUE;
            }
        });
    }

    private void init() throws IOException {
        if (this.logger.level <= 800) {
            this.logger.log("Initing directories");
        }

        this.initDirectories();
        if (this.logger.level <= 800) {
            this.logger.log("Initing directory map");
        }

        this.initDirectoryMap(this.appDirectory);
        if (this.logger.level <= 800) {
            this.logger.log("Initing files");
        }

        this.initFiles(this.appDirectory);
        if (this.logger.level <= 800) {
            this.logger.log("Initing file map");
        }

        this.initFileMap(this.appDirectory);
        if (this.logger.level <= 800) {
            this.logger.log("Syncing metadata");
        }

        if (this.index) {
            this.writeDirty();
        }

        if (this.logger.level <= 800) {
            this.logger.log("Done initing");
        }

    }

    private void initDirectories() throws IOException {
        this.rootDirectory = new File(this.rootDir);
        createDirectory(this.rootDirectory);
        this.backupDirectory = new File(this.rootDirectory, "/FreePastry-Storage-Root/");
        createDirectory(this.backupDirectory);
        this.appDirectory = new File(this.backupDirectory, this.getName());
        createDirectory(this.appDirectory);
        this.lostDirectory = new File(this.backupDirectory, "lost+found");
        createDirectory(this.lostDirectory);
    }

    private void initDirectoryMap(File dir) {
        File[] files = dir.listFiles(new NewPersistentStorage.DirectoryFilter());
        this.directories.put(dir, files);

        for(int i = 0; i < files.length; ++i) {
            this.initDirectoryMap(files[i]);
        }

    }

    private void initFiles(File dir) throws IOException {
        String[] dirs = dir.list(new NewPersistentStorage.DirectoryFilter());
        String[] files = dir.list(new NewPersistentStorage.FileFilter());

        int i;
        for(i = 0; i < files.length; ++i) {
            try {
                if (!this.initTemporaryFile(dir, files[i]) && dirs.length > 0) {
                    this.moveFileToCorrectDirectory(dir, files[i]);
                }
            } catch (Exception var6) {
                if (this.logger.level <= 900) {
                    this.logger.logException("Got exception " + var6 + " initting file " + files[i] + " - moving to lost+found.", var6);
                }

                this.moveToLost(new File(dir, files[i]));
            }
        }

        for(i = 0; i < dirs.length; ++i) {
            this.initFiles(new File(dir, dirs[i]));
        }

        if (dirs.length > 0) {
            deleteFile(new File(dir, "metadata.cache"));
        }

    }

    private boolean initTemporaryFile(File parent, String name) throws IOException {
        if (!this.isTemporaryFile(name)) {
            return false;
        } else {
            this.moveToLost(new File(parent, name));
            return true;
        }
    }

    private void initFileMap(File dir) throws IOException {
        if (this.logger.level <= 500) {
            this.logger.log("Initting directory " + dir);
        }

        this.checkDirectory(dir);
        if (dir.exists()) {
            long modified = 0L;
            if (this.index) {
                try {
                    modified = this.readMetadataFile(dir);
                } catch (IOException var11) {
                    if (this.logger.level <= 1000) {
                        this.logger.logException("Got exception " + var11 + " reading metadata file - regenerating", var11);
                    }
                }
            }

            File[] files = dir.listFiles(new NewPersistentStorage.FileFilter());
            File[] dirs = dir.listFiles(new NewPersistentStorage.DirectoryFilter());

            int i;
            for(i = 0; i < files.length; ++i) {
                try {
                    Id id = this.readKey(files[i]);
                    long len = getFileLength(files[i]);
                    if (id == null && this.logger.level <= 800) {
                        this.logger.log("READING " + files[i] + " RETURNED NULL!");
                    }

                    if (len > 0L) {
                        this.increaseUsedSpace(len);
                        if (this.index && (!this.metadata.containsKey(id) || files[i].lastModified() > modified)) {
                            if (this.logger.level <= 400) {
                                this.logger.log("Reading newer metadata out of file " + files[i] + " id " + id.toStringFull() + " " + files[i].lastModified() + " " + modified + " " + this.metadata.containsKey(id));
                            }

                            this.metadata.put(id, this.readMetadata(files[i]));
                            this.dirty.add(dir);
                        }
                    } else {
                        this.moveToLost(files[i]);
                        if (this.index && this.metadata.containsKey(id)) {
                            this.metadata.remove(id);
                            this.dirty.add(dir);
                        }
                    }
                } catch (Exception var10) {
                    if (this.logger.level <= 900) {
                        this.logger.logException("ERROR: Received Exception " + var10 + " while initing file " + files[i] + " - moving to lost+found.", var10);
                    }

                    this.moveToLost(files[i]);
                }
            }

            for(i = 0; i < dirs.length; ++i) {
                this.initFileMap(dirs[i]);
            }

            this.checkDirectory(dir);
        }
    }

    private void resolveConflict(File file1, File file2, File output) throws IOException {
        if (!file2.exists()) {
            renameFile(file1, output);
        } else if (!file1.exists()) {
            renameFile(file2, output);
        } else if (file1.equals(file2)) {
            renameFile(file1, output);
        } else {
            if (this.logger.level <= 500) {
                this.logger.log("resolving conflict between " + file1 + " and " + file2);
            }

            if (readVersion(file1) < readVersion(file2)) {
                this.moveToLost(file1);
                renameFile(file2, output);
            } else {
                this.moveToLost(file2);
                renameFile(file1, output);
            }
        }

    }

    private void moveToLost(File file) throws IOException {
        renameFile(file, new File(this.lostDirectory, this.getPrefix(file.getParentFile()) + file.getName()));
    }

    private boolean checkDirectory(File directory) throws IOException {
        int files = this.numFilesDir(directory);
        int dirs = this.numDirectoriesDir(directory);
        if (this.logger.level <= 500) {
            this.logger.log("Checking directory " + directory + " for oversize " + files + "/" + dirs);
        }

        if (files > 256) {
            this.expandDirectory(directory);
            return true;
        } else if (dirs > 32) {
            this.reformatDirectory(directory);
            return true;
        } else if (files == 0 && dirs == 0 && !directory.equals(this.appDirectory)) {
            this.pruneDirectory(directory);
            return true;
        } else {
            return false;
        }
    }

    private void pruneDirectory(File dir) throws IOException {
        if (this.logger.level <= 500) {
            this.logger.log("Pruning directory " + dir + " due to emptiness");
        }

        deleteFile(new File(dir, "metadata.cache"));
        deleteDirectory(dir);
        this.directories.remove(dir);
        this.prefixes.remove(dir);
        this.directories.put(dir.getParentFile(), dir.getParentFile().listFiles(new NewPersistentStorage.DirectoryFilter()));
    }

    private void reformatDirectory(File dir) throws IOException {
        if (this.logger.level <= 500) {
            this.logger.log("Expanding directory " + dir + " due to too many subdirectories");
        }

        String[] newDirNames = this.getDirectories(dir.list(new NewPersistentStorage.DirectoryFilter()));
        this.reformatDirectory(dir, newDirNames);
        if (this.logger.level <= 500) {
            this.logger.log("Done expanding directory " + dir);
        }

    }

    private void reformatDirectory(File dir, String[] newDirNames) throws IOException {
        String[] dirNames = dir.list(new NewPersistentStorage.DirectoryFilter());
        File[] newDirs = new File[newDirNames.length];

        for(int i = 0; i < newDirNames.length; ++i) {
            newDirs[i] = new File(dir, newDirNames[i]);
            createDirectory(newDirs[i]);
            if (this.logger.level <= 500) {
                this.logger.log("Creating directory " + newDirNames[i]);
            }

            String[] subDirNames = this.getMatchingDirectories(newDirNames[i], dirNames);
            File[] newSubDirs = new File[subDirNames.length];

            for(int j = 0; j < subDirNames.length; ++j) {
                File oldDir = new File(dir, subDirNames[j]);
                newSubDirs[j] = new File(newDirs[i], subDirNames[j].substring(newDirNames[i].length()));
                if (this.logger.level <= 500) {
                    this.logger.log("Moving the old direcotry " + oldDir + " to " + newSubDirs[j]);
                }

                renameFile(oldDir, newSubDirs[j]);
                this.directories.remove(oldDir);
                this.directories.put(newSubDirs[j], new File[0]);
            }

            this.directories.put(newDirs[i], newSubDirs);
        }

        this.directories.put(dir, newDirs);
    }

    private String[] getMatchingDirectories(String prefix, String[] dirNames) {
        Vector result = new Vector();

        for(int i = 0; i < dirNames.length; ++i) {
            if (dirNames[i].startsWith(prefix)) {
                result.add(dirNames[i]);
            }
        }

        return (String[])((String[])result.toArray(new String[0]));
    }

    private void expandDirectory(File dir) throws IOException {
        if (this.logger.level <= 500) {
            this.logger.log("Expanding directory " + dir + " due to too many files");
        }

        String[] fileNames = dir.list(new NewPersistentStorage.FileFilter());
        String[] dirNames = this.getDirectories(fileNames);
        File[] dirs = new File[dirNames.length];

        for(int i = 0; i < dirNames.length; ++i) {
            dirs[i] = new File(dir, dirNames[i]);
            this.directories.put(dirs[i], new File[0]);
            if (dirs[i].exists() && dirs[i].isFile()) {
                renameFile(dirs[i], new File(dir, dirs[i].getName() + "!"));
            }

            createDirectory(dirs[i]);
            if (this.logger.level <= 500) {
                this.logger.log("Creating directory " + dirNames[i]);
            }

            if (this.index) {
                this.dirty.add(dirs[i]);
            }
        }

        this.directories.put(dir, dirs);
        File[] files = dir.listFiles(new NewPersistentStorage.FileFilter());

        for(int i = 0; i < files.length; ++i) {
            for(int j = 0; j < dirs.length; ++j) {
                if (files[i].getName().startsWith(dirs[j].getName())) {
                    if (this.logger.level <= 300) {
                        this.logger.log("Renaming file " + files[i] + " to " + new File(dirs[j], files[i].getName().substring(dirs[j].getName().length())));
                    }

                    renameFile(files[i], new File(dirs[j], files[i].getName().substring(dirs[j].getName().length())));
                    break;
                }
            }
        }

        deleteFile(new File(dir, "metadata.cache"));
        if (this.logger.level <= 500) {
            this.logger.log("Done expanding directory " + dir);
        }

    }

    private String[] getDirectories(String[] names) {
        int length = this.getPrefixLength(names);
        String prefix = names[0].substring(0, length);
        NewPersistentStorage.CharacterHashSet set = new NewPersistentStorage.CharacterHashSet();

        for(int i = 0; i < names.length; ++i) {
            if (names[i].length() > length) {
                set.put(names[i].charAt(length));
            }
        }

        char[] splits = set.get();
        String[] result = new String[splits.length];

        for(int i = 0; i < result.length; ++i) {
            result[i] = prefix + splits[i];
        }

        return result;
    }

    private int getPrefixLength(String[] names) {
        int length = names[0].length() - 1;

        for(int i = 0; i < names.length; ++i) {
            length = this.getPrefixLength(names[0], names[i], length);
        }

        return length;
    }

    private int getPrefixLength(String a, String b, int max) {
        int i;
        for(i = 0; i < a.length() - 1 && i < b.length() - 1 && i < max; ++i) {
            if (a.charAt(i) != b.charAt(i)) {
                return i;
            }
        }

        return i;
    }

    private void moveFileToCorrectDirectory(File parent, String name) throws IOException {
        File file = new File(parent, name);
        Id id = this.readKeyFromFile(file);
        File dest = this.getDirectoryForId(id);
        if (!dest.equals(parent)) {
            if (this.logger.level <= 500) {
                this.logger.log("moving file " + file + " to correct directory " + dest + " from " + parent);
            }

            File other = new File(dest, id.toStringFull().substring(this.getPrefix(dest).length()));
            this.resolveConflict(file, other, other);
            this.checkDirectory(dest);
        }

    }

    private void flushDirectory(File dir) throws IOException {
        if (this.logger.level <= 500) {
            this.logger.log("Flushing file " + dir);
        }

        if (!dir.isDirectory()) {
            Id id = this.readKey(dir);
            if (this.index) {
                synchronized(this.metadata) {
                    this.metadata.remove(id);
                }
            }

            this.decreaseUsedSpace(dir.length());
            deleteFile(dir);
        } else {
            File[] dirs = dir.listFiles();

            for(int i = 0; i < dirs.length; ++i) {
                this.flushDirectory(dirs[i]);
                this.directories.remove(dirs[i]);
                this.prefixes.remove(dirs[i]);
                deleteFile(dirs[i]);
            }
        }

    }

    private static void createDirectory(File directory) throws IOException {
        if (directory == null || directory.exists() && directory.isFile() || !directory.exists() && !directory.mkdirs()) {
            throw new IOException("Creation of directory " + directory + " failed!");
        }
    }

    private static void deleteDirectory(File directory) throws IOException {
        if (directory != null && directory.exists()) {
            if (directory.listFiles().length > 0) {
                throw new IOException("Cannot delete " + directory + " - directory is not empty!");
            }

            if (!directory.delete()) {
                throw new IOException("Deletion of directory " + directory + " failed!");
            }
        }

    }

    private static long getFileLength(File file) {
        return file != null && file.exists() ? file.length() : 0L;
    }

    private static void renameFile(File oldFile, File newFile) throws IOException {
        if (oldFile != null && oldFile.exists() && !oldFile.equals(newFile)) {
            deleteFile(newFile);
            if (!oldFile.renameTo(newFile)) {
                throw new IOException("Rename of " + oldFile + " to " + newFile + " failed!");
            }
        }

    }

    private static void deleteFile(File file) throws IOException {
        if (file != null && file.exists() && !file.delete()) {
            throw new IOException("Delete of " + file + " failed!");
        }
    }

    private boolean isTemporaryFile(String name) {
        return name.indexOf(".") >= 0;
    }

    private File makeTemporaryFile(Id id) throws IOException {
        File directory = this.getDirectoryForId(id);

        File file;
        for(file = new File(directory, id.toStringFull().substring(this.getPrefix(directory).length()) + "." + this.environment.getRandomSource().nextInt() % 100); file.exists(); file = new File(directory, id.toStringFull().substring(this.getPrefix(directory).length()) + "." + this.environment.getRandomSource().nextInt() % 100)) {
        }

        return file;
    }

    private boolean isAncestor(File file, File ancestor) {
        while(file != null && !file.equals(ancestor)) {
            file = file.getParentFile();
        }

        return file != null;
    }

    private File getFile(Id id) throws IOException {
        File dir = this.getDirectoryForId(id);
        String name = id.toStringFull().substring(this.getPrefix(dir).length());
        if (name.equals("")) {
            name = "!";
        }

        File file = new File(dir, name);
        if (file.exists() && file.isDirectory()) {
            file = new File(file, "!");
        }

        return file;
    }

    private File getDirectoryForId(Id id) throws IOException {
        return this.getDirectoryForName(id.toStringFull());
    }

    private File getDirectoryForName(String name) throws IOException {
        return this.getDirectoryForName(name, this.appDirectory);
    }

    private File getDirectoryForName(String name, File dir) throws IOException {
        File[] subDirs = (File[])((File[])this.directories.get(dir));
        if (subDirs.length == 0) {
            return dir;
        } else {
            for(int i = 0; i < subDirs.length; ++i) {
                if (name.startsWith(subDirs[i].getName())) {
                    return this.getDirectoryForName(name.substring(subDirs[i].getName().length()), subDirs[i]);
                }

                if (name.length() == 0 && subDirs[i].getName().equals("!")) {
                    return this.getDirectoryForName(name, subDirs[i]);
                }
            }

            if (name.length() >= subDirs[0].getName().length() || name.length() == 0 && subDirs[0].getName().length() == 1) {
                File newDir = new File(dir, name.length() == 0 ? "!" : name.substring(0, subDirs[0].getName().length()));
                if (this.logger.level <= 500) {
                    this.logger.log("Necessarily creating dir " + newDir.getName());
                }

                createDirectory(newDir);
                this.directories.put(dir, this.append(subDirs, newDir));
                this.directories.put(newDir, new File[0]);
                return this.checkDirectory(dir) ? this.getDirectoryForName(name, dir) : newDir;
            } else {
                String[] dirs = new String[subDirs.length + 1];

                for(int i = 0; i < subDirs.length; ++i) {
                    dirs[i] = subDirs[i].getName();
                }

                dirs[subDirs.length] = name.length() == 0 ? "!" : name;
                this.reformatDirectory(dir, this.getDirectories(dirs));
                return this.getDirectoryForName(name, dir);
            }
        }
    }

    private String getPostfix(Id id, File file) {
        return id.toStringFull().substring(this.getPrefix(file).length());
    }

    private String getPrefix(File file) {
        if (this.prefixes.get(file) != null) {
            return (String)this.prefixes.get(file);
        } else {
            StringBuffer buffer;
            for(buffer = new StringBuffer(); !file.equals(this.appDirectory); file = file.getParentFile()) {
                buffer.insert(0, file.getName().replaceAll("!", ""));
            }

            this.prefixes.put(file, buffer.toString());
            return this.getPrefix(file);
        }
    }

    private File[] append(File[] files, File file) {
        File[] result = new File[files.length + 1];

        for(int i = 0; i < files.length; ++i) {
            result[i] = files[i];
        }

        result[files.length] = file;
        return result;
    }

    private int numDirectoriesDir(File dir) {
        return dir.listFiles(new NewPersistentStorage.DirectoryFilter()).length;
    }

    private int numFilesDir(File dir) {
        return dir.listFiles(new NewPersistentStorage.FileFilter()).length;
    }

    private boolean isFile(File parent, String name) {
        return !(new File(parent, name)).isDirectory() && !name.equals("metadata.cache");
    }

    private boolean isDirectory(File parent, String name) {
        return (new File(parent, name)).isDirectory();
    }

    private boolean containsDir(File dir) {
        return dir.listFiles(new NewPersistentStorage.DirectoryFilter()).length != 0;
    }

    protected void writeDirty() {
        File[] files = (File[])((File[])this.dirty.toArray(new File[0]));

        for(int i = 0; i < files.length; ++i) {
            HashMap map = new HashMap();
            IdRange range = this.getRangeForDirectory(files[i]);
            Iterator keys = null;
            if (range.getCCWId().compareTo(range.getCWId()) <= 0) {
                keys = this.metadata.keySubMap(range.getCCWId(), range.getCWId()).keySet().iterator();
            } else {
                keys = this.metadata.keyTailMap(range.getCCWId()).keySet().iterator();
            }

            while(keys.hasNext()) {
                Id next = (Id)keys.next();
                map.put(next, this.metadata.get(next));
            }

            try {
                writeMetadataFile(files[i], map);
                synchronized(this.metadata) {
                    this.dirty.remove(files[i]);
                }
            } catch (FileNotFoundException var13) {
                FileNotFoundException f = var13;

                try {
                    synchronized(this.metadata) {
                        this.dirty.remove(files[i]);
                    }

                    if (this.logger.level <= 900) {
                        this.logger.logException("ERROR: Could not find directory while writing out metadata in '" + files[i].getCanonicalPath() + "' - removing from dirty list and continuing!", f);
                    }
                } catch (IOException var12) {
                    if (this.logger.level <= 1000) {
                        this.logger.logException("PANIC: Got IOException " + var12 + " trying to detail FNF exception " + var13 + " while writing out file " + files[i], var12);
                    }
                }
            } catch (IOException var14) {
                IOException e = var14;

                try {
                    if (this.logger.level <= 900) {
                        this.logger.logException("ERROR: Got error " + e + " while writing out metadata in '" + files[i].getCanonicalPath() + "' - aborting!", e);
                    }
                } catch (IOException var11) {
                    if (this.logger.level <= 1000) {
                        this.logger.logException("PANIC: Got IOException " + var11 + " trying to detail exception " + var14 + " while writing out file " + files[i], var11);
                    }
                }
            }
        }

    }

    private long readMetadataFile(File file) throws IOException {
        File metadata = new File(file, "metadata.cache");
        if (!metadata.exists()) {
            return -1L;
        } else {
            FileInputStream fin = null;

            long var7;
            try {
                fin = new FileInputStream(metadata);
                ObjectInputStream objin = new ObjectInputStream(new BufferedInputStream(fin));
                IdRange range = this.getRangeForDirectory(file);

                try {
                    HashMap map = (HashMap)objin.readObject();
                    Iterator keys = map.keySet().iterator();

                    while(keys.hasNext()) {
                        Id id = (Id)keys.next();
                        if (range.containsId(id) && (new File(file, id.toStringFull().substring(this.getPrefix(file).length()))).exists()) {
                            this.metadata.put(id, map.get(id));
                        } else {
                            this.dirty.add(file);
                        }
                    }

                    long var18 = metadata.lastModified();
                    return var18;
                } catch (ClassNotFoundException var14) {
                    if (this.logger.level <= 900) {
                        this.logger.logException("ERROR: Got exception " + var14 + " while reading metadata file " + metadata + " - rebuilding file", var14);
                    }

                    deleteFile(metadata);
                    var7 = 0L;
                    return var7;
                } catch (IOException var15) {
                    if (this.logger.level <= 900) {
                        this.logger.logException("ERROR: Got exception " + var15 + " while reading metadata file " + metadata + " - rebuilding file", var15);
                    }

                    deleteFile(metadata);
                    var7 = 0L;
                }
            } finally {
                fin.close();
            }

            return var7;
        }
    }

    private static void writeMetadataFile(File file, HashMap map) throws IOException {
        FileOutputStream fout = null;

        try {
            fout = new FileOutputStream(new File(file, "metadata.cache"));
            ObjectOutputStream objout = new ObjectOutputStream(new BufferedOutputStream(fout));
            objout.writeObject(map);
            objout.close();
        } catch (IOException var7) {
            throw new JavaSerializationException(map, var7);
        } finally {
            if (fout != null) {
                fout.close();
            }

        }

    }

    protected IdRange getRangeForDirectory(File dir) {
        String result;
        for(result = ""; !dir.equals(this.appDirectory); dir = dir.getParentFile()) {
            result = dir.getName() + result;
        }

        return this.factory.buildIdRangeFromPrefix(result);
    }

    private static Serializable readObject(File file, int offset) throws IOException {
        FileInputStream fin = null;

        try {
            fin = new FileInputStream(file);
            ObjectInputStream objin = new NewXMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(fin)));

            for(int i = 0; i < offset; ++i) {
                objin.readObject();
            }

            Serializable var10 = (Serializable)objin.readObject();
            return var10;
        } catch (ClassNotFoundException var8) {
            throw new IOException(var8.getMessage());
        } finally {
            fin.close();
        }
    }

    private static Serializable readData(File file) throws IOException {
        return readObject(file, 1);
    }

    private Serializable readMetadata(File file) throws IOException {
        if (file.length() < 32L) {
            return null;
        } else {
            RandomAccessFile ras = null;

            Object var3;
            try {
                ras = new RandomAccessFile(file, "r");
                ras.seek(file.length() - 32L);
                if (ras.readLong() == 8038844221L) {
                    if (ras.readLong() != 2L) {
                        if (this.logger.level <= 900) {
                            this.logger.log("Persistence version did not match - exiting!");
                        }

                        var3 = null;
                        return (Serializable)var3;
                    }

                    if (ras.readLong() > 1L) {
                        if (this.logger.level <= 900) {
                            this.logger.log("Persistence revision did not match - exiting!");
                        }

                        var3 = null;
                        return (Serializable)var3;
                    }

                    long length = ras.readLong();
                    ras.seek(file.length() - 32L - length);
                    FileInputStream fis = null;

                    try {
                        fis = new FileInputStream(ras.getFD());
                        ObjectInputStream objin = new XMLObjectInputStream(new BufferedInputStream(new GZIPInputStream(fis)));

                        Object e;
                        try {
                            e = (Serializable)objin.readObject();
                            return (Serializable)e;
                        } catch (ClassNotFoundException var16) {
                            e = var16;
                            throw new IOException(var16.getMessage());
                        }
                    } finally {
                        fis.close();
                    }
                }

                var3 = null;
            } finally {
                ras.close();
            }

            return (Serializable)var3;
        }
    }

    private Id readKey(File file) {
        String s = this.getPrefix(file.getParentFile()) + file.getName().replaceAll("!", "");
        return s.indexOf(".") >= 0 ? this.factory.buildIdFromToString(s.toCharArray(), 0, s.indexOf(".")) : this.factory.buildIdFromToString(s.toCharArray(), 0, s.length());
    }

    private Id readKeyFromFile(File file) throws IOException {
        return (Id)readObject(file, 0);
    }

    private static long readVersion(File file) throws IOException {
        Long temp = (Long)readObject(file, 2);
        return temp == null ? 0L : temp;
    }

    private static long writeObject(Serializable obj, Serializable metadata, Id key, long version, File file) throws IOException {
        FileOutputStream fout = null;

        try {
            fout = new FileOutputStream(file);
            ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
            objout.writeObject(key);
            objout.writeObject(obj);
            objout.writeObject(version);
            objout.close();
        } finally {
            if (fout != null) {
                fout.close();
            }

        }

        long len1 = file.length();

        try {
            fout = new FileOutputStream(file, true);
            ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
            objout.writeObject(metadata);
            objout.close();
        } finally {
            fout.close();
        }

        long len2 = file.length();

        try {
            fout = new FileOutputStream(file, true);
            DataOutputStream dos = new DataOutputStream(fout);
            dos.writeLong(8038844221L);
            dos.writeLong(2L);
            dos.writeLong(1L);
            dos.writeLong(len2 - len1);
            dos.close();
        } finally {
            fout.close();
        }

        return file.length();
    }

    private static void writeMetadata(File file, Serializable metadata) throws IOException {
        RandomAccessFile ras = null;
        FileOutputStream fout = null;
        long len1;
        if (file.length() > 32L) {
            try {
                ras = new RandomAccessFile(file, "rw");
                ras.seek(file.length() - 32L);
                if (ras.readLong() == 8038844221L && ras.readLong() == 2L && ras.readLong() <= 1L) {
                    len1 = ras.readLong();
                    ras.setLength(file.length() - 32L - len1);
                }
            } finally {
                ras.close();
            }
        }

        len1 = file.length();

        try {
            fout = new FileOutputStream(file, true);
            ObjectOutputStream objout = new XMLObjectOutputStream(new BufferedOutputStream(new GZIPOutputStream(fout)));
            objout.writeObject(metadata);
            objout.close();
        } finally {
            fout.close();
        }

        long len2 = file.length();

        try {
            fout = new FileOutputStream(file, true);
            DataOutputStream dos = new DataOutputStream(fout);
            dos.writeLong(8038844221L);
            dos.writeLong(2L);
            dos.writeLong(1L);
            dos.writeLong(len2 - len1);
            dos.close();
        } finally {
            fout.close();
        }

    }

    public boolean setRoot(String dir) {
        this.rootDir = dir;
        return true;
    }

    public String getRoot() {
        return this.rootDir;
    }

    public long getStorageSize() {
        return this.storageSize > 0L ? this.storageSize : Long.MAX_VALUE;
    }

    public boolean setStorageSize(long size) {
        if (this.storageSize <= size) {
            this.storageSize = size;
            return true;
        } else if (size > this.usedSize) {
            this.storageSize = size;
            return true;
        } else {
            return false;
        }
    }

    private void increaseUsedSpace(long i) {
        this.usedSize += i;
    }

    private void decreaseUsedSpace(long i) {
        this.usedSize -= i;
    }

    private long getUsedSpace() {
        return this.usedSize;
    }

    public String getName() {
        return this.name;
    }

    private static class OutofDiskSpaceException extends NewPersistentStorage.PersistenceException {
        private OutofDiskSpaceException() {
            super();
        }
    }

    private static class PersistenceException extends Exception {
        private PersistenceException() {
        }
    }

    private class CharacterHashSet {
        protected boolean[] bitMap;

        private CharacterHashSet() {
            this.bitMap = new boolean[256];
        }

        public void put(char a) {
            this.bitMap[a] = true;
        }

        public boolean contains(char a) {
            return this.bitMap[a];
        }

        public void remove(char a) {
            this.bitMap[a] = false;
        }

        public char[] get() {
            int[] nums = this.getOffsets();
            char[] result = new char[nums.length];

            for(int i = 0; i < result.length; ++i) {
                result[i] = (char)nums[i];
            }

            return result;
        }

        private int[] getOffsets() {
            int[] result = new int[this.count()];

            for(int i = 0; i < result.length; ++i) {
                result[i] = this.getOffset(i);
            }

            return result;
        }

        private int getOffset(int index) {
            int location;
            for(location = 0; index > 0; ++location) {
                if (this.bitMap[location]) {
                    --index;
                }
            }

            while(!this.bitMap[location]) {
                ++location;
            }

            return location;
        }

        private int count() {
            int total = 0;

            for(int i = 0; i < this.bitMap.length; ++i) {
                if (this.bitMap[i]) {
                    ++total;
                }
            }

            return total;
        }
    }

    private class FileFilter implements FilenameFilter {
        private FileFilter() {
        }

        public boolean accept(File dir, String name) {
            return NewPersistentStorage.this.isFile(dir, name);
        }
    }

    private class DirectoryFilter implements FilenameFilter {
        private DirectoryFilter() {
        }

        public boolean accept(File dir, String name) {
            return NewPersistentStorage.this.isDirectory(dir, name);
        }
    }
}

