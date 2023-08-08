# Twitter Dataset

To use the Twitter dataset and convert it to Neo4j format, please follow these steps.

1. Download `twitter_rv_orig.net` from http://an.kaist.ac.kr/traces/WWW2010.html into the `inputs/` folder. If the file is unavailable, please contact us for our local copy.
2. Run `python nodes-list.py` to generate a list of all Twitter users in the graph.
3. Run `python relationships-list.py` to generate a list of all "follow" relationships in the graph.
4. Download a compiled Neo4j version, or compile yourself from source, and update `NEO4J_ADMIN_LOCATION` in `creategraph.sh` to point to the neo4j-admin binary.
5. Run `./creategraph.sh` to import nodes and relationships into a new Neo4j graph.
