#!/bin/bash

# This will need to be changed to point to the compiled neo4j-admin executable
NEO4J_ADMIN_LOCATION="neo4j-community-4.0.1-SNAPSHOT/bin/neo4j-admin"

INPUT_LOCATION="inputs/"
INPUT_USERS_HEADER="users-header.csv"
INPUT_USERS_FILE="users.csv"
INPUT_RELS_HEADER="rels-header.csv"
INPUT_RELS_FILE="rels.csv"

OUTPUT_GRAPH_NAME="twitter-users.db"

$NEO4J_ADMIN_LOCATION import \
    --nodes=User="${INPUT_LOCATION}${INPUT_USERS_HEADER},${INPUT_LOCATION}${INPUT_USERS_FILE}" \
    --relationships=FOLLOWS="${INPUT_LOCATION}${INPUT_RELS_HEADER},${INPUT_LOCATION}${INPUT_RELS_FILE}"
