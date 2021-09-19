# Distributed Systems Course - Assignment 1 RPCs

A scalable distributed system in Java for computing the bcrypt key derivation function, with the help of Apache Thrift RPC.

#### Architecture

The system comprises a client layer, a front end (FE) layer, and a back end (BE) layer. The FE layer accepts connections from clients and forwards requests to the BE layer in a manner that balances load. The BE layer is distributed horizontally for scalability, and the FE layer is centralized for simplicity.