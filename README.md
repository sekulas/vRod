# vRod: A Vector Database
> This database was developed as part of my engineering thesis - Project & Implementation of Vector Database.

The implementation was created using the **Rust** programming language and required implementing
essential database system functionalities such as data storage, **B+tree** based indexing, query processing,
and transaction management using the write-ahead logging (**WAL**) mechanism. Vector space searching, a fundamental feature of vector databases,
was implemented using the Hierarchical Navigable Small World (**HNSW**) algorithm, which applies the concept of approximate nearest-neighbor search (**ANNS**),
enabling efficient similarity searches between vectors. 
The system’s architecture follows the ”**Command**” and ”Command and Query Responsibility Segregation” (**CQRS**) design patterns.

# Use Cases

<img src="https://github.com/user-attachments/assets/e0f5998a-7c57-494b-be86-fa0c8152d9f4" width="60%">

*Visualization showcasing the use cases of the vRod vector database.*

# Architecture

<img src="https://github.com/user-attachments/assets/8bc3afb6-d72a-4990-839a-3584ff1eafe2" width="40%">

*Diagram illustrating the high-level architecture of the vRod database.*

### Command & CQRS Design Patterns

<img src="https://github.com/user-attachments/assets/3e1a688a-9f58-489c-bcea-11156f3539b0" width="50%">

*Visualization of how the Command and CQRS design patterns are implemented to separate write and read operations for better scalability and maintainability.*

### File System

<img src="https://github.com/user-attachments/assets/82d5e6e3-5398-4094-8d86-3a5d17c54660" width="60%">

*Depiction of the file system structure utilized by the system.*

### Storage

<img src="https://github.com/user-attachments/assets/5daf2de6-5c68-44ad-93d9-f8e9024419c2" width="70%">

*Detailed view of the storage -- used structures and data layout.*

### WAL - Transaction Management

<img src="https://github.com/user-attachments/assets/28911529-2be2-4522-a6b7-f7e20ddf8d09" width="60%">

*Detailed view of the WAL -- used structures and data layout.*

### B+tree - Index

<img src="https://github.com/user-attachments/assets/86c2a926-b5cd-4b8b-a7e3-08cf2bdd71e8" width="80%">

*Detailed view of the modified B+tree -- used structures and data layout.*

<img src="https://github.com/user-attachments/assets/419e563c-8ee6-411d-a821-a0b129a3cea6" width="60%">

*Example of how data insertion is handled within the implemented modified B+tree structure.*

### HNSW - Vector Index

<img src="https://github.com/user-attachments/assets/6d34cf89-f06c-46b0-9098-907cb06cbd05" width="30%">

*Visualization of selecting the next neighbor using the heuristic method[^1].*

<img src="https://github.com/user-attachments/assets/5a4eb8b6-efe4-4e1b-bb88-41b5ad932cb1" width="40%">

*Visualization of the in-memory HNSW graph structure during the construction phase.*

<img src="https://github.com/user-attachments/assets/48bb128e-63b9-4137-af0e-74a360741656" width="60%">

*Sequential visualization of the HNSW index construction.*

<img src="https://github.com/user-attachments/assets/907a84b9-1554-4ec5-8810-53ef87b06090" width="40%">

*Visualization of the operation of searching for similar points in HNSW[^2].*

# Bibliography

[^1]: Malkov, Y. A., and Yashunin, D. A.,"Efficient and Robust Approximate Nearest Neighbor Search Using Hierarchical Navigable Small World Graphs," IEEE Transactions on Pattern Analysis and Machine Intelligence, vol. 42, no. 4, pp. 824–836, Apr. 2020, ISSN: 0162-8828, 2160-9292, 1939-3539. doi: 10.1109 TPAMI.2018.2889473. Accessed Nov. 17, 2024. Available at: https://ieeexplore.ieee.org/document/8594636/.

[^2]: https://www.pinecone.io/learn/series/faiss/hnsw (Accessed Dec. 14, 2024).
