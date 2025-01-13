# Table of Contents

1. [vRod: A Vector Database](#vrod-a-vector-database)
2. [Use Cases](#use-cases)
3. [Architecture](#architecture)
   - [Main Components](#main-components)
   - [Command & CQRS Design Patterns](#command--cqrs-design-patterns)
   - [File System](#file-system)
   - [Storage](#storage)
   - [WAL - Transaction Management](#wal---transaction-management)
   - [B+tree - Index](#btree---index)
   - [HNSW - Vector Index](#hnsw---vector-index)
4. [Examples](#examples)
   - [1. Solution Architect for a Vector Database PoC](#1-solution-architect-for-a-vector-database-poc)
   - [2. Large Language Model Developer Customizing Search Results](#2-large-language-model-developer-customizing-search-results)
5. [Help and Documentation](#help-and-documentation)
6. [Bibliography](#bibliography)

# vRod: A Vector Database
> This database was developed as part of my engineering thesis - Design and Implementation of a Vector Database.

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

### Main Components

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

# Examples

### 1. Solution Architect for a Vector Database PoC

#### Objective

Identify a vector database for a Proof of Concept (PoC) that is efficient, scalable, user-friendly, and capable of accurate vector similarity search.

#### Process

Environment Setup:

```zsh
vrod -i . -n "sift_db"
Database created at: "./sift_db"

vrod -e "CREATE" -a "sift_1m_col" -d sift_db 
Executing command: "CREATE sift_1m_col"
Collection created at: "sift_db/sift_1m_col"
```

Data Insertion: Using the ANN_SIFT1M dataset[^3] (1 million 128-dimensional vectors).

```zsh
vrod -e "BULK_INSERT" -f sift_dataset/sift1m_input.txt -d sift_db -c sift_1m_col
Parsing vectors and payloads from file...
Vectors and payloads parsed in 3.4844644s.
Executing command: "BULK_INSERT"
Collecting vectors and payloads...
Vectors and payloads collected.
Inserting vectors and payloads...
Vectors and payloads inserted in 3.932022s.
```

Index Creation: Euclidean distance-based vector index.

```zsh
vrod -e "CREATE_VECTOR_INDEX" -a "EUCLID" -d sift_db -c sift_1m_col
Executing command: "CREATE_VECTOR_INDEX EUCLID"
Creating HNSW Index...
HNSW Index created in 284.26566s.
```

Similarity Search:

```zsh
vrod -e "SEARCH_SIMILAR" -a "EUCLID 1.0,(...)" -d sift_db -c sift_1m_col
Executing query: "SEARCH_SIMILAR EUCLID"
Loading HNSW Index...
HNSW Index loaded in 0.27991423s.
Searching similar vectors...
id        | similarity   | payload
--------- | ------------ | -------
932086    | 232.87122    | 932085
934877    | 234.71472    | 934876
561814    | 243.98976    | 561813
708178    | 255.46037    | 708177
706772    | 256.31427    | 706771
695757    | 258.86288    | 695756
435346    | 261.24127    | 435345
701259    | 264.28015    | 701258
455538    | 267.2845     | 455537
872729    | 268.06903    | 872728
```
#### Results

The vRod database effectively handled 1 million vectors.

Accurate nearest neighbor search with efficient operations.

### 2. Large Language Model Developer Customizing Search Results

#### Objective

Enable customization of similarity search results to align with specific preferences.

#### Process

Environment Setup:

```zsh
vrod -i . -n "db"
Database created at: "./db"

vrod -e "CREATE" -a "top_uni" -d db 
Executing command: "CREATE top_uni"
Collection created at: "db/top_uni"
```

Data Vectorization and Insertion: Using all-MiniLM-L6-v2[^4] for embeddings.

```zsh
vrod -g 20 -a $'\n' -f to_be_embedded.txt 
File content acquired.
Embeddings length: 20
Embedding dimension: 384
Size of the embeddings file: 0.09 MB

vrod -e "BULK_INSERT" -f embeddings.txt -c top_uni -d db 
Parsing vectors and payloads from file...
Vectors and payloads parsed in 0.001920939s.
Executing command: "BULK_INSERT"
Collecting vectors and payloads...
Vectors and payloads collected.
Inserting vectors and payloads...
Vectors and payloads inserted in 0.00743906s.

vrod -e "CREATE_VECTOR_INDEX" -a "COSINE" -c top_uni -d db 
Executing command: "CREATE_VECTOR_INDEX COSINE"
Creating HNSW Index...
HNSW Index created in 0.003545391s.
```

Initial Similarity Search:

```zsh
vrod -e "SEARCH_SIMILAR" -c top_uni -d db -a "COSINE -0.03908783,(...)"
Executing query: "SEARCH_SIMILAR COSINE"
Loading HNSW Index...
HNSW Index loaded in 0.000171699s.
Searching similar vectors...
id        | similarity   | payload
--------- | ------------ | -------
1         | 0.9999999    | What is the best university in poland?
2         | 0.9130575    | University of Warsaw
5         | 0.87840545   | Warsaw University of Technology
14        | 0.86457586   | Poznań University of Technology
9         | 0.8604667    | Gdańsk University of Technology
3         | 0.86006516   | Jagiellonian University in Kraków
13        | 0.8541289    | Łódź University of Technology
11        | 0.83903986   | Adam Mickiewicz University in Poznań
12        | 0.83505535   | Wrocław University of Technology
7         | 0.8201228    | AGH University of Science and Technology in Kraków
```

Record Modification: Adjust search results to reflect preferences.

```zsh
vrod -e "UPDATE" -a "2;;Warsaw University of Technology" -c top_uni -d db 
Executing command: "UPDATE 2;;Warsaw University of Technology"
Embedding updated successfully.

vrod -e "UPDATE" -a "5;;University of Warsaw" -c top_uni -d db
Executing command: "UPDATE 5;;University of Warsaw"
Embedding updated successfully.
```

Post-Modification Search:

```zsh
vrod -e "SEARCH_SIMILAR" -c top_uni -d db -a "COSINE -0.03908783,(...)"
Executing query: "SEARCH_SIMILAR COSINE"
Loading HNSW Index...
HNSW Index loaded in 0.000234967s.
Searching similar vectors...
id        | similarity   | payload
--------- | ------------ | -------
1         | 0.9999999    | What is the best university in poland?
2         | 0.9130575    | Warsaw University of Technology
5         | 0.87840545   | University of Warsaw
14        | 0.86457586   | Poznań University of Technology
9         | 0.8604667    | Gdańsk University of Technology
3         | 0.86006516   | Jagiellonian University in Kraków
13        | 0.8541289    | Łódź University of Technology
11        | 0.83903986   | Adam Mickiewicz University in Poznań
12        | 0.83505535   | Wrocław University of Technology
7         | 0.8201228    | AGH University of Science and Technology in Kraków
```

#### Results

Successful customization of search results based on preferences.

Efficient and flexible data manipulation within the vRod database.

# Help and Documentation
To display the user guide and available commands, run:
```zsh
vrod
vrod -h
vrod --help
```

# Bibliography

[^1]: Malkov, Y. A., and Yashunin, D. A.,"Efficient and Robust Approximate Nearest Neighbor Search Using Hierarchical Navigable Small World Graphs," IEEE Transactions on Pattern Analysis and Machine Intelligence, vol. 42, no. 4, pp. 824–836, Apr. 2020, ISSN: 0162-8828, 2160-9292, 1939-3539. doi: 10.1109 TPAMI.2018.2889473. Accessed Nov. 17, 2024. Available at: https://ieeexplore.ieee.org/document/8594636/.

[^2]: https://www.pinecone.io/learn/series/faiss/hnsw (Accessed Dec. 14, 2024).

[^3]: Available at: http://corpus-texmex.irisa.fr (Accessed Dec. 14, 2024)

[^4]: Available at: https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2 (Accessed Dec. 9, 2024).
