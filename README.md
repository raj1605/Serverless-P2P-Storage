# Serverless-P2P-Storage

This repo is for buildingand running the P2P storage for serverless applications. It consists of three main folders - 1) Controller Application (containing the orchestrator node application), 2) Client (containing the client SDK in Python and few experiments), 3) demo1 (Consists of the data nodes code)

## How to buid the data node
You can choose one of the two methods below,
1. Using Docker
2. By building and running the application using maven

### 1. Using Docker

The dockerfiles are available in the repo raj1605/projectwarehouse. Please choose the tcp tag to get the up to date project. and run it. We need to send the application arguments whick denotes information on the ports, IP addresses that the datanodes have to run on, replication factor, in-memory, and the mode of communication (tcp/rest).

For example, docker run -p 8080:8080 raj1605/project-warehouse:tcp 9001 172.17.0.2 9001 5 true 3 tcp (5 denotes the number of nodes, 3 denotes the replication factor)

This should spawn the datanodes.

### 2. By maven building

cd into the project file into demo1. Maven install the requried dependency files from the jars/ folder. The required packages can be known from pom.xml.Once you finish installing the maven dependencies, the libraries are in the local repo in the .m2 folder. After this package the whole application to a jar using maven. Run the jar using Java 17 with the application arguments just like for the docker method.

## How to build the controller node

Controller nodes are built using the maven build tools. Run the packaged jar using Java 17. This should keep the controller node up. The application is listening in the port 8000.

## How to start using the system

The Controller.py can be used to connect and issue get/put requests to the storage system.

The users need to add a few extra commands to use the storage,

- First is to register the application by calling register()
- Second is to open a TCP connection with the datanodes. Using open_tcp().
- Then the regular put/get requests can be issues, using get(), put() for the REST endpoints, get_tcp(), put_tcp() for the TCP communications. We can issue multiple requests as long as the tcp connection is open.
- In case of TCP, we need to close the TCP connection using close().