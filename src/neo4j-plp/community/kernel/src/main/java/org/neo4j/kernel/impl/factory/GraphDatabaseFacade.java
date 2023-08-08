/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.factory;

import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.security.URLAccessValidationError;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeIndexCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.IOUtils;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TokenAccess;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.core.TokenNotFoundException;
import org.neo4j.kernel.impl.coreapi.AutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.IndexManagerImpl;
import org.neo4j.kernel.impl.coreapi.IndexProviderImpl;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.coreapi.PlaceboTransaction;
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker;
import org.neo4j.kernel.impl.coreapi.ReadOnlyIndexFacade;
import org.neo4j.kernel.impl.coreapi.ReadOnlyRelationshipIndexFacade;
import org.neo4j.kernel.impl.coreapi.RelationshipAutoIndexerFacade;
import org.neo4j.kernel.impl.coreapi.TopLevelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.SchemaImpl;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.transaction_timeout;
import static org.neo4j.helpers.collection.Iterators.emptyResourceIterator;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing.NODE_AUTO_INDEX;
import static org.neo4j.kernel.impl.api.explicitindex.InternalAutoIndexing.RELATIONSHIP_AUTO_INDEX;
import static org.neo4j.values.storable.Values.utf8Value;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.graphdb.ResourceIterator;

/**
 * Implementation of the GraphDatabaseService/GraphDatabaseService interfaces - the "Core API". Given an {@link SPI}
 * implementation, this provides users with
 * a clean facade to interact with the database.
 */
public class GraphDatabaseFacade implements GraphDatabaseAPI, EmbeddedProxySPI {
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();

    private Schema schema;
    private Supplier<IndexManager> indexManager;
    private ThreadToStatementContextBridge statementContext;
    private SPI spi;
    private TransactionalContextFactory contextFactory;
    private Config config;
    private TokenHolders tokenHolders;

    /**
     * This is what you need to implement to get your very own {@link GraphDatabaseFacade}. This SPI exists as a thin
     * layer to make it easy to provide
     * alternate {@link org.neo4j.graphdb.GraphDatabaseService} instances without having to re-implement this whole API
     * implementation.
     */
    public interface SPI {
        /**
         * Check if database is available, waiting up to {@code timeout} if it isn't. If the timeout expires before
         * database available, this returns false
         */
        boolean databaseIsAvailable(long timeout);

        DependencyResolver resolver();

        StoreId storeId();

        DatabaseLayout databaseLayout();

        /**
         * Eg. Neo4j Enterprise HA, Neo4j Community Standalone..
         */
        String name();

        void shutdown();

        /**
         * Begin a new kernel transaction with specified timeout in milliseconds.
         *
         * @throws org.neo4j.graphdb.TransactionFailureException if unable to begin, or a transaction already exists.
         * @see GraphDatabaseAPI#beginTransaction(KernelTransaction.Type, LoginContext)
         */
        KernelTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext, long timeout);

        /**
         * Execute a cypher statement
         */
        Result executeQuery(String query, MapValue parameters, TransactionalContext context);

        AutoIndexing autoIndexing();

        void registerKernelEventHandler(KernelEventHandler handler);

        void unregisterKernelEventHandler(KernelEventHandler handler);

        <T> void registerTransactionEventHandler(TransactionEventHandler<T> handler);

        <T> void unregisterTransactionEventHandler(TransactionEventHandler<T> handler);

        URL validateURLAccess(URL url) throws URLAccessValidationError;

        GraphDatabaseQueryService queryService();

        Kernel kernel();
    }

    public GraphDatabaseFacade() {
    }

    /**
     * Create a new Core API facade, backed by the given SPI and using pre-resolved dependencies
     */
    public void init(SPI spi, ThreadToStatementContextBridge txBridge, Config config, TokenHolders tokenHolders) {
        this.spi = spi;
        this.config = config;
        this.schema = new SchemaImpl(() -> txBridge.getKernelTransactionBoundToThisThread(true));
        this.statementContext = txBridge;
        this.tokenHolders = tokenHolders;
        this.indexManager = Suppliers.lazySingleton(() ->
        {
            IndexProviderImpl idxProvider = new IndexProviderImpl(this, () -> txBridge.getKernelTransactionBoundToThisThread(true));
            AutoIndexerFacade<Node> nodeAutoIndexer = new AutoIndexerFacade<>(
                    () -> new ReadOnlyIndexFacade<>(idxProvider.getOrCreateNodeIndex(NODE_AUTO_INDEX, null)),
                    spi.autoIndexing().nodes());
            RelationshipAutoIndexerFacade relAutoIndexer = new RelationshipAutoIndexerFacade(
                    () -> new ReadOnlyRelationshipIndexFacade(
                            idxProvider.getOrCreateRelationshipIndex(RELATIONSHIP_AUTO_INDEX, null)),
                    spi.autoIndexing().relationships());

            return new IndexManagerImpl(() -> txBridge.getKernelTransactionBoundToThisThread(true), idxProvider,
                    nodeAutoIndexer, relAutoIndexer);
        });

        this.contextFactory = Neo4jTransactionalContextFactory.create(spi, txBridge, locker);
    }

    @Override
    public Node createNode() {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        try (Statement ignore = transaction.acquireStatement()) {
            return newNodeProxy(transaction.dataWrite().nodeCreate());
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Long createNodeId() {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        try (Statement ignore = transaction.acquireStatement()) {
            return transaction.dataWrite().nodeCreate();
        } catch (InvalidTransactionTypeKernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Node createNode(Label... labels) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        try (Statement ignore = transaction.acquireStatement()) {
            TokenWrite tokenWrite = transaction.tokenWrite();
            int[] labelIds = new int[labels.length];
            String[] labelNames = new String[labels.length];
            for (int i = 0; i < labelNames.length; i++) {
                labelNames[i] = labels[i].name();
            }
            tokenWrite.labelGetOrCreateForNames(labelNames, labelIds);

            Write write = transaction.dataWrite();
            long nodeId = write.nodeCreateWithLabels(labelIds);

            return newNodeProxy(nodeId);
        } catch (ConstraintValidationException e) {
            throw new ConstraintViolationException("Unable to add label.", e);
        } catch (SchemaKernelException e) {
            throw new IllegalArgumentException(e);
        } catch (KernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

    @Override
    public Node getNodeById(long id) {
        if (id < 0) {
            throw new NotFoundException(format("Node %d not found", id),
                    new EntityNotFoundException(EntityType.NODE, id));
        }

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread(true);
        assertTransactionOpen(ktx);
        try (Statement ignore = ktx.acquireStatement()) {
            if (!ktx.dataRead().nodeExists(id)) {
                throw new NotFoundException(format("Node %d not found", id),
                        new EntityNotFoundException(EntityType.NODE, id));
            }
            return newNodeProxy(id);
        }
    }

    @Override
    public Relationship getRelationshipById(long id) {
        if (id < 0) {
            throw new NotFoundException(format("Relationship %d not found", id),
                    new EntityNotFoundException(EntityType.RELATIONSHIP, id));
        }

        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread(true);
        assertTransactionOpen(ktx);
        try (Statement ignore = statementContext.get()) {
            if (!ktx.dataRead().relationshipExists(id)) {
                throw new NotFoundException(format("Relationship %d not found", id),
                        new EntityNotFoundException(EntityType.RELATIONSHIP, id));
            }
            return newRelationshipProxy(id);
        }
    }

    @Deprecated
    @Override
    public IndexManager index() {
        return indexManager.get();
    }

    @Override
    public Schema schema() {
        assertTransactionOpen();
        return schema;
    }

    @Override
    public boolean isAvailable(long timeoutMillis) {
        return spi.databaseIsAvailable(timeoutMillis);
    }

    @Override
    public void shutdown() {
        spi.shutdown();
    }

    @Override
    public Transaction beginTx() {
        return beginTransaction(KernelTransaction.Type.explicit, AUTH_DISABLED);
    }

    @Override
    public Transaction beginTx(long timeout, TimeUnit unit) {
        return beginTransaction(KernelTransaction.Type.explicit, AUTH_DISABLED, timeout, unit);
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext) {
        return beginTransactionInternal(type, loginContext, config.get(transaction_timeout).toMillis());
    }

    @Override
    public InternalTransaction beginTransaction(KernelTransaction.Type type, LoginContext loginContext,
                                                long timeout, TimeUnit unit) {
        return beginTransactionInternal(type, loginContext, unit.toMillis(timeout));
    }

    @Override
    public Result execute(String query) throws QueryExecutionException {
        return execute(query, Collections.emptyMap());
    }

    @Override
    public Result execute(String query, long timeout, TimeUnit unit) throws QueryExecutionException {
        return execute(query, Collections.emptyMap(), timeout, unit);
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters) throws QueryExecutionException {
        // ensure we have a tx and create a context (the tx is gonna get closed by the Cypher result)
        InternalTransaction transaction =
                beginTransaction(KernelTransaction.Type.implicit, AUTH_DISABLED);

        return execute(transaction, query, ValueUtils.asParameterMapValue(parameters));
    }

    @Override
    public Result execute(String query, Map<String, Object> parameters, long timeout, TimeUnit unit) throws
            QueryExecutionException {
        InternalTransaction transaction =
                beginTransaction(KernelTransaction.Type.implicit, AUTH_DISABLED, timeout, unit);
        return execute(transaction, query, ValueUtils.asParameterMapValue(parameters));
    }

    public Result execute(InternalTransaction transaction, String query, MapValue parameters)
            throws QueryExecutionException {
        TransactionalContext context =
                contextFactory.newContext(ClientConnectionInfo.EMBEDDED_CONNECTION, transaction, query, parameters);
        return spi.executeQuery(query, parameters, context);
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread(true);
        assertTransactionOpen(ktx);
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            NodeCursor cursor = ktx.cursors().allocateNodeCursor();
            ktx.dataRead().allNodesScan(cursor);
            return new PrefetchingResourceIterator<Node>() {
                @Override
                protected Node fetchNextOrNull() {
                    if (cursor.next()) {
                        return newNodeProxy(cursor.nodeReference());
                    } else {
                        close();
                        return null;
                    }
                }

                @Override
                public void close() {
                    cursor.close();
                    statement.close();
                }
            };
        };
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread(true);
        assertTransactionOpen(ktx);
        return () ->
        {
            Statement statement = ktx.acquireStatement();
            RelationshipScanCursor cursor = ktx.cursors().allocateRelationshipScanCursor();
            ktx.dataRead().allRelationshipsScan(cursor);
            return new PrefetchingResourceIterator<Relationship>() {
                @Override
                protected Relationship fetchNextOrNull() {
                    if (cursor.next()) {
                        return newRelationshipProxy(
                                cursor.relationshipReference(),
                                cursor.sourceNodeReference(),
                                cursor.type(),
                                cursor.targetNodeReference());
                    } else {
                        close();
                        return null;
                    }
                }

                @Override
                public void close() {
                    cursor.close();
                    statement.close();
                }
            };
        };
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse() {
        return allInUse(TokenAccess.LABELS);
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse() {
        return allInUse(TokenAccess.RELATIONSHIP_TYPES);
    }

    private <T> ResourceIterable<T> allInUse(final TokenAccess<T> tokens) {
        assertTransactionOpen();
        return () -> tokens.inUse(statementContext.getKernelTransactionBoundToThisThread(true));
    }

    @Override
    public ResourceIterable<Label> getAllLabels() {
        return all(TokenAccess.LABELS);
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes() {
        return all(TokenAccess.RELATIONSHIP_TYPES);
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys() {
        return all(TokenAccess.PROPERTY_KEYS);
    }

    private <T> ResourceIterable<T> all(final TokenAccess<T> tokens) {
        assertTransactionOpen();
        return () ->
        {
            KernelTransaction transaction =
                    statementContext.getKernelTransactionBoundToThisThread(true);
            return tokens.all(transaction);
        };
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler) {
        spi.registerKernelEventHandler(handler);
        return handler;
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler) {
        spi.registerTransactionEventHandler(handler);
        return handler;
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler) {
        spi.unregisterKernelEventHandler(handler);
        return handler;
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler) {
        spi.unregisterTransactionEventHandler(handler);
        return handler;
    }

    @Override
    public ResourceIterator<Node> findNodes(final Label myLabel, final String key, final Object value) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(myLabel.name());
        int propertyId = tokenRead.propertyKey(key);
        return nodesByLabelAndProperty(transaction, labelId, IndexQuery.exact(propertyId, Values.of(value)));
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        return nodesByLabelAndProperties(transaction, labelId,
                IndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1)),
                IndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2)));
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String key1, Object value1, String key2, Object value2,
                                            String key3, Object value3) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        return nodesByLabelAndProperties(transaction, labelId,
                IndexQuery.exact(tokenRead.propertyKey(key1), Values.of(value1)),
                IndexQuery.exact(tokenRead.propertyKey(key2), Values.of(value2)),
                IndexQuery.exact(tokenRead.propertyKey(key3), Values.of(value3)));
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, Map<String, Object> propertyValues) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(label.name());
        IndexQuery.ExactPredicate[] queries = new IndexQuery.ExactPredicate[propertyValues.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : propertyValues.entrySet()) {
            queries[i++] = IndexQuery.exact(tokenRead.propertyKey(entry.getKey()), Values.of(entry.getValue()));
        }
        return nodesByLabelAndProperties(transaction, labelId, queries);
    }

    @Override
    public ResourceIterator<Node> findNodes(
            final Label myLabel, final String key, final String value, final StringSearchMode searchMode) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(myLabel.name());
        int propertyId = tokenRead.propertyKey(key);
        IndexQuery query;
        switch (searchMode) {
            case EXACT:
                query = IndexQuery.exact(propertyId, utf8Value(value.getBytes(UTF_8)));
                break;
            case PREFIX:
                query = IndexQuery.stringPrefix(propertyId, utf8Value(value.getBytes(UTF_8)));
                break;
            case SUFFIX:
                query = IndexQuery.stringSuffix(propertyId, utf8Value(value.getBytes(UTF_8)));
                break;
            case CONTAINS:
                query = IndexQuery.stringContains(propertyId, utf8Value(value.getBytes(UTF_8)));
                break;
            default:
                throw new IllegalStateException("Unknown string search mode: " + searchMode);
        }
        return nodesByLabelAndProperty(transaction, labelId, query);
    }

    @Override
    public Node findNode(final Label myLabel, final String key, final Object value) {
        try (ResourceIterator<Node> iterator = findNodes(myLabel, key, value)) {
            if (!iterator.hasNext()) {
                return null;
            }
            Node node = iterator.next();
            if (iterator.hasNext()) {
                throw new MultipleFoundException(
                        format("Found multiple nodes with label: '%s', property name: '%s' and property " +
                                "value: '%s' while only one was expected.", myLabel, key, value));
            }
            return node;
        }
    }

    @Override
    public ResourceIterator<Node> findNodes(final Label myLabel) {
        return allNodesWithLabel(myLabel);
    }

    private InternalTransaction beginTransactionInternal(KernelTransaction.Type type, LoginContext loginContext,
                                                         long timeoutMillis) {
        if (statementContext.hasTransaction()) {
            // FIXME: perhaps we should check that the new type and access mode are compatible with the current tx
            return new PlaceboTransaction(statementContext.getKernelTransactionBoundToThisThread(true));
        }
        return new TopLevelTransaction(spi.beginTransaction(type, loginContext, timeoutMillis));
    }

    private ResourceIterator<Node> nodesByLabelAndProperty(KernelTransaction transaction, int labelId, IndexQuery query) {
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();

        if (query.propertyKeyId() == TokenRead.NO_TOKEN || labelId == TokenRead.NO_TOKEN) {
            statement.close();
            return emptyResourceIterator();
        }
        IndexReference index = transaction.schemaRead().index(labelId, query.propertyKeyId());
        if (index != IndexReference.NO_INDEX) {
            // Ha! We found an index - let's use it to find matching nodes
            try {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                read.nodeIndexSeek(index, cursor, IndexOrder.NONE, false, query);

                return new NodeCursorResourceIterator<>(cursor, statement, this::newNodeProxy);
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutIndex(statement, labelId, query);
    }

    private ResourceIterator<Node> nodesByLabelAndProperties(
            KernelTransaction transaction, int labelId, IndexQuery.ExactPredicate... queries) {
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();

        if (isInvalidQuery(labelId, queries)) {
            statement.close();
            return emptyResourceIterator();
        }

        int[] propertyIds = getPropertyIds(queries);
        IndexReference index = findMatchingIndex(transaction, labelId, propertyIds);

        if (index != IndexReference.NO_INDEX) {
            try {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                read.nodeIndexSeek(index, cursor, IndexOrder.NONE, false, getReorderedIndexQueries(index.properties(), queries));
                return new NodeCursorResourceIterator<>(cursor, statement, this::newNodeProxy);
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }
        return getNodesByLabelAndPropertyWithoutIndex(statement, labelId, queries);
    }

    private static IndexReference findMatchingIndex(KernelTransaction transaction, int labelId, int[] propertyIds) {
        IndexReference index = transaction.schemaRead().index(labelId, propertyIds);
        if (index != IndexReference.NO_INDEX) {
            // index found with property order matching the query
            return index;
        } else {
            // attempt to find matching index with different property order
            Arrays.sort(propertyIds);
            assertNoDuplicates(propertyIds, transaction.tokenRead());

            int[] workingCopy = new int[propertyIds.length];

            Iterator<IndexReference> indexes = transaction.schemaRead().indexesGetForLabel(labelId);
            while (indexes.hasNext()) {
                index = indexes.next();
                int[] original = index.properties();
                if (hasSamePropertyIds(original, workingCopy, propertyIds)) {
                    // Ha! We found an index with the same properties in another order
                    return index;
                }
            }
            return IndexReference.NO_INDEX;
        }
    }

    private static IndexQuery[] getReorderedIndexQueries(int[] indexPropertyIds, IndexQuery[] queries) {
        IndexQuery[] orderedQueries = new IndexQuery[queries.length];
        for (int i = 0; i < indexPropertyIds.length; i++) {
            int propertyKeyId = indexPropertyIds[i];
            for (IndexQuery query : queries) {
                if (query.propertyKeyId() == propertyKeyId) {
                    orderedQueries[i] = query;
                    break;
                }
            }
        }
        return orderedQueries;
    }

    private static boolean hasSamePropertyIds(int[] original, int[] workingCopy, int[] propertyIds) {
        if (original.length == propertyIds.length) {
            System.arraycopy(original, 0, workingCopy, 0, original.length);
            Arrays.sort(workingCopy);
            return Arrays.equals(propertyIds, workingCopy);
        }
        return false;
    }

    private static int[] getPropertyIds(IndexQuery[] queries) {
        int[] propertyIds = new int[queries.length];
        for (int i = 0; i < queries.length; i++) {
            propertyIds[i] = queries[i].propertyKeyId();
        }
        return propertyIds;
    }

    private static boolean isInvalidQuery(int labelId, IndexQuery[] queries) {
        boolean invalidQuery = labelId == TokenRead.NO_TOKEN;
        for (IndexQuery query : queries) {
            int propertyKeyId = query.propertyKeyId();
            invalidQuery = invalidQuery || propertyKeyId == TokenRead.NO_TOKEN;
        }
        return invalidQuery;
    }

    private static void assertNoDuplicates(int[] propertyIds, TokenRead tokenRead) {
        int prev = propertyIds[0];
        for (int i = 1; i < propertyIds.length; i++) {
            int curr = propertyIds[i];
            if (curr == prev) {
                SilentTokenNameLookup tokenLookup = new SilentTokenNameLookup(tokenRead);
                throw new IllegalArgumentException(
                        format("Provided two queries for property %s. Only one query per property key can be performed",
                                tokenLookup.propertyKeyGetName(curr)));
            }
            prev = curr;
        }
    }

    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndex(
            Statement statement, int labelId, IndexQuery... queries) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor();
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor();
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor();

        transaction.dataRead().nodeLabelScan(labelId, nodeLabelCursor);

        return new NodeLabelPropertyIterator(transaction.dataRead(),
                nodeLabelCursor,
                nodeCursor,
                propertyCursor,
                statement,
                this::newNodeProxy,
                queries);
    }

    private ResourceIterator<Node> allNodesWithLabel(final Label myLabel) {
        KernelTransaction ktx = statementContext.getKernelTransactionBoundToThisThread(true);
        Statement statement = ktx.acquireStatement();

        int labelId = ktx.tokenRead().nodeLabel(myLabel.name());
        if (labelId == TokenRead.NO_TOKEN) {
            statement.close();
            return Iterators.emptyResourceIterator();
        }

        NodeLabelIndexCursor cursor = ktx.cursors().allocateNodeLabelIndexCursor();
        ktx.dataRead().nodeLabelScan(labelId, cursor);
        return new NodeCursorResourceIterator<>(cursor, statement, this::newNodeProxy);
    }

    @Override
    public TraversalDescription traversalDescription() {
        return new MonoDirectionalTraversalDescription(statementContext);
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        return new BidirectionalTraversalDescriptionImpl(statementContext);
    }

    // GraphDatabaseAPI
    @Override
    public DependencyResolver getDependencyResolver() {
        return spi.resolver();
    }

    @Override
    public StoreId storeId() {
        return spi.storeId();
    }

    @Override
    public URL validateURLAccess(URL url) throws URLAccessValidationError {
        return spi.validateURLAccess(url);
    }

    @Override
    public DatabaseLayout databaseLayout() {
        return spi.databaseLayout();
    }

    @Override
    public String toString() {
        return spi.name() + " [" + databaseLayout() + "]";
    }

    @Override
    public Statement statement() {
        return statementContext.get();
    }

    @Override
    public KernelTransaction kernelTransaction() {
        return statementContext.getKernelTransactionBoundToThisThread(true);
    }

    @Override
    public GraphDatabaseService getGraphDatabase() {
        return this;
    }

    @Override
    public void assertInUnterminatedTransaction() {
        statementContext.assertInUnterminatedTransaction();
    }

    @Override
    public void failTransaction() {
        statementContext.getKernelTransactionBoundToThisThread(true).failure();
    }

    @Override
    public RelationshipProxy newRelationshipProxy(long id) {
        return new RelationshipProxy(this, id);
    }

    @Override
    public RelationshipProxy newRelationshipProxy(long id, long startNodeId, int typeId, long endNodeId) {
        return new RelationshipProxy(this, id, startNodeId, typeId, endNodeId);
    }

    @Override
    public NodeProxy newNodeProxy(long nodeId) {
        return new NodeProxy(this, nodeId);
    }

    @Override
    public RelationshipType getRelationshipTypeById(int type) {
        try {
            String name = tokenHolders.relationshipTypeTokens().getTokenById(type).name();
            return RelationshipType.withName(name);
        } catch (TokenNotFoundException e) {
            throw new IllegalStateException("Kernel API returned non-existent relationship type: " + type);
        }
    }

    @Override
    public GraphPropertiesProxy newGraphPropertiesProxy() {
        return new GraphPropertiesProxy(this);
    }

    private static class NodeLabelPropertyIterator extends PrefetchingNodeResourceIterator {
        private final Read read;
        private final NodeLabelIndexCursor nodeLabelCursor;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        private final IndexQuery[] queries;

        NodeLabelPropertyIterator(
                Read read,
                NodeLabelIndexCursor nodeLabelCursor,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                Statement statement,
                NodeFactory nodeFactory,
                IndexQuery... queries) {
            super(statement, nodeFactory);
            this.read = read;
            this.nodeLabelCursor = nodeLabelCursor;
            this.nodeCursor = nodeCursor;
            this.propertyCursor = propertyCursor;
            this.queries = queries;
        }

        @Override
        protected long fetchNext() {
            boolean hasNext;
            do {
                hasNext = nodeLabelCursor.next();

            } while (hasNext && !hasPropertiesWithValues());

            if (hasNext) {
                return nodeLabelCursor.nodeReference();
            } else {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources(Statement statement) {
            IOUtils.closeAllSilently(statement, nodeLabelCursor, nodeCursor, propertyCursor);
        }

        private boolean hasPropertiesWithValues() {
            int targetCount = queries.length;
            read.singleNode(nodeLabelCursor.nodeReference(), nodeCursor);
            if (nodeCursor.next()) {
                nodeCursor.properties(propertyCursor);
                while (propertyCursor.next()) {
                    for (IndexQuery query : queries) {
                        if (propertyCursor.propertyKey() == query.propertyKeyId()) {
                            if (query.acceptsValueAt(propertyCursor)) {
                                targetCount--;
                                if (targetCount == 0) {
                                    return true;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    private void assertTransactionOpen() {
        assertTransactionOpen(statementContext.getKernelTransactionBoundToThisThread(true));
    }

    private static void assertTransactionOpen(KernelTransaction transaction) {
        if (transaction.isTerminated()) {
            Status terminationReason = transaction.getReasonIfTerminated().orElse(Status.Transaction.Terminated);
            throw new TransactionTerminatedException(terminationReason);
        }
    }

    private static final class NodeCursorResourceIterator<CURSOR extends NodeIndexCursor> extends PrefetchingNodeResourceIterator {
        private final CURSOR cursor;

        NodeCursorResourceIterator(CURSOR cursor, Statement statement, NodeFactory nodeFactory) {
            super(statement, nodeFactory);
            this.cursor = cursor;
        }

        @Override
        long fetchNext() {
            if (cursor.next()) {
                return cursor.nodeReference();
            } else {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources(Statement statement) {
            IOUtils.closeAllSilently(statement, cursor);
        }
    }

    private abstract static class PrefetchingNodeResourceIterator implements ResourceIterator<Node> {
        private final Statement statement;
        private final NodeFactory nodeFactory;
        private long next;
        private boolean closed;

        private static final long NOT_INITIALIZED = -2L;
        protected static final long NO_ID = -1L;

        PrefetchingNodeResourceIterator(Statement statement, NodeFactory nodeFactory) {
            this.statement = statement;
            this.nodeFactory = nodeFactory;
            this.next = NOT_INITIALIZED;
        }

        @Override
        public boolean hasNext() {
            if (next == NOT_INITIALIZED) {
                next = fetchNext();
            }
            return next != NO_ID;
        }

        @Override
        public Node next() {
            if (!hasNext()) {
                close();
                throw new NoSuchElementException();
            }
            Node nodeProxy = nodeFactory.make(next);
            next = fetchNext();
            return nodeProxy;
        }

        @Override
        public void close() {
            if (!closed) {
                next = NO_ID;
                closeResources(statement);
                closed = true;
            }
        }

        abstract long fetchNext();

        abstract void closeResources(Statement statement);
    }

    private interface NodeFactory {
        NodeProxy make(long id);
    }



    /* ============================================================================================================================================== */
    /* ============================================================================================================================================== */
    /* ================================================================ My additions ================================================================ */
    /* ============================================================================================================================================== */
    /* ============================================================================================================================================== */

    /* ========================= CONSTANTS ========================= */

    public static int NODES_TO_PROP = 45000; // Number of nodes to propagate
    public static int NUM_THREADS = 32; // Number of propagation threads

    private static boolean ENABLE_BATCHING_DEBUG_OUTPUT = false;
    private static boolean ENABLE_OPERATION_DEBUG_OUTPUT = false;
    private static boolean ENABLE_FUSION_DEBUG_OUTPUT = false;


    public void setParams(final int NODES_TO_PROP) {
        this.NODES_TO_PROP = NODES_TO_PROP;
    }
    public void setNumThreads(int num_threads) {
        this.NUM_THREADS = num_threads;
        System.out.println(" ===== Set number of threads to " + NUM_THREADS + " ===== ");
        this.thread_pool = Executors.newFixedThreadPool(NUM_THREADS);
    }
   
    public void setBatchingDebugOutput(boolean enable) {
        this.ENABLE_BATCHING_DEBUG_OUTPUT = enable;
    }
    public void setOperationDebugOutput(boolean enable) {
        this.ENABLE_OPERATION_DEBUG_OUTPUT = enable;
    }
    public void setFusionDebugOutput(boolean enable) {
        this.ENABLE_FUSION_DEBUG_OUTPUT = enable;
    }


    // Timer for throughput timing
    private static class Timer {
        long cumulative;
        long started_at;

        public Timer() {
            this.cumulative = 0;
            this.started_at = -1;
        }

        public void start() {
            this.started_at = System.currentTimeMillis();
        }
        public void stop() {
            if(started_at == -1) {
                return;
            }
            long stopped_at = System.currentTimeMillis();
            cumulative += (stopped_at - started_at);
            started_at = -1;
        }
        public long getTimeInMillis() {
            return cumulative;
        }
        public void reset() {
            this.cumulative = 0;
            this.started_at = -1;
        }
    }

    private boolean timingEnabled = false;
    private Timer timer = new Timer();
    public void startTimer() {
        this.timingEnabled = true;
        this.timer.start();
    }
    public void pauseTimer() {
        this.timer.stop();
    }
    public long stopTimer() {
        this.timer.stop();
        this.timingEnabled = false;
        long time = this.timer.getTimeInMillis();
        this.timer.reset();
        return time;
    }

    private static class Pair<T, R> {
        T first;
        R second;

        Pair(T first, R second) {
            this.first = first;
            this.second = second;
        }
    }

    /* ========================= ITERATOR CLASSES ========================= */

    // The below iterators are used for traversal and batching
    
    private abstract static class BatchedPrefetchingNodeResourceIterator implements ResourceIterator<Node> {
        private final Statement statement;
        private final NodeFactory nodeFactory;
        private long next;
        private boolean closed;

        protected static final long NOT_FINISHED = -3L;
        private static final long NOT_INITIALIZED = -2L;
        protected static final long NO_ID = -1L;

        BatchedPrefetchingNodeResourceIterator(Statement statement, NodeFactory nodeFactory) {
            this.statement = statement;
            this.nodeFactory = nodeFactory;
            this.next = NOT_INITIALIZED;
        }

        @Override
        public boolean hasNext() {
            return this.next != NO_ID;
        }

        @Override
        public Node next() {
            throw new UnsupportedOperationException("Not implemented");
        }


        @Override
        public void close() {
            if (!closed) {
                next = NO_ID;
                closeResources(statement);
                closed = true;
            }
        }

        abstract long fetchNext();

        abstract void closeResources(Statement statement);
    }

    private static class BatchedNodeLabelPropertyIterator extends BatchedPrefetchingNodeResourceIterator {
        private final Read read;
        private final NodeLabelIndexCursor nodeLabelCursor;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        public IndexQuery[] queries;
        public long numberOfNodesViewed;

        BatchedNodeLabelPropertyIterator(
                Read read,
                NodeLabelIndexCursor nodeLabelCursor,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                Statement statement,
                NodeFactory nodeFactory,
                IndexQuery... queries
        ) {
            super(statement, nodeFactory);
            this.read = read;
            this.nodeLabelCursor = nodeLabelCursor;
            this.nodeCursor = nodeCursor;
            this.propertyCursor = propertyCursor;
            this.queries = queries;
            this.numberOfNodesViewed = 0;
        }

        public void addQueries(IndexQuery[] queries_to_add) {
            // TODO: Optimize
            IndexQuery[] newQueries = new IndexQuery[queries.length + queries_to_add.length];
            for (int i = 0; i < queries.length; i++) {
                newQueries[i] = this.queries[i];
            }
            for(int i = queries.length; i < queries.length + queries_to_add.length; i++) {
                newQueries[i] = queries_to_add[i-queries.length];
            }
            queries = newQueries;
        }

        private void removeFromQueries(Object key) {
            // Now only removes first
            IndexQuery[] newQueries = new IndexQuery[queries.length - 1];
            int i, j;
            boolean found = false;
            for (i = 0, j = 0; i < queries.length; i++) {
                if (!found && queries[i].acceptsValue(Values.of(key))) {
                    found = true;
                    continue;
                }
                newQueries[j] = this.queries[i];
                j++;
            }
            queries = newQueries;
        }

        @Override
        public boolean hasNext() {
            return super.hasNext() && this.queries.length > 0;
        }

        protected long fetchNext() {
            throw new UnsupportedOperationException("Not implemented");
        }

        public HashMap<Object, Long> get() {
            boolean hasNext;
            HashMap<Object, Long> found = new HashMap<>();
            Pair<Object, Long> prop;
            int count = 0;
            do {
                hasNext = nodeLabelCursor.next();
                prop = hasPropertiesWithValues();
                if(prop != null) {
                    found.put(prop.first, prop.second);
                    this.removeFromQueries(prop.first);
                    prop = null;
                }

                numberOfNodesViewed++;
                count++;
            } while (count < NODES_TO_PROP && hasNext && this.queries.length > 0);
            if(!hasNext) {
                this.close();
            }
            return found;
        }

        @Override
        void closeResources(Statement statement) {
            IOUtils.closeAllSilently(statement, nodeLabelCursor, nodeCursor, propertyCursor);
        }

        private Pair hasPropertiesWithValues() {
            read.singleNode(nodeLabelCursor.nodeReference(), nodeCursor);
            if (nodeCursor.next()) {
                nodeCursor.properties(propertyCursor);
                while (propertyCursor.next()) {
                    for (IndexQuery query : queries) {
                        if (propertyCursor.propertyKey() == query.propertyKeyId()) {
                            if (query.acceptsValueAt(propertyCursor)) {
				finishedTimes.put(propertyCursor.propertyValue().asObject(), System.currentTimeMillis());
                                return new Pair<>(propertyCursor.propertyValue().asObject(), nodeCursor.nodeReference());
                            }
                        }
                    }
                }
            }
            return null;
	}
    }

    static ConcurrentHashMap<Object, Long> finishedTimes = new ConcurrentHashMap<>();


    /* ========================= OPERATION CLASSES ========================= */


    private class Add extends Operation {
        long id;

        public Add(long id) {
            super();
            this.id = id;
        }

        @Override
        public boolean isBatchedFind() {
            return false;
        }

        @Override
        public void handleRemove() {}

        @Override
        public boolean isSpliced() {
            return false;
        }
    }

    private class Delete extends Operation {
        long id;

        public Delete(long id) {
            super();
            this.id = id;
        }

        @Override
        public boolean isBatchedFind() {
            return false;
        }

        @Override
        public void handleRemove() {
        }

        @Override
        public boolean isSpliced() {
            return false;
        }
    }

    private class Find extends Operation {
        Label myLabel;
        String key;
        Object value;
        boolean spliced;

        public Find(Label myLabel, String key, Splice value) {
            super();
            this.myLabel = myLabel;
            this.key = key;
            this.value = value;
            this.spliced = true;
        }

        public Find(Label myLabel, String key, Object value) {
            super();
            this.myLabel = myLabel;
            this.key = key;
            this.value = value;
            this.spliced = false;
        }

        @Override
        public boolean isBatchedFind() {
            return false;
        }

        @Override
        public void handleRemove() {
        }

        @Override
        public boolean isSpliced() {
            return this.spliced;
        }

        @Override
        public String toString() {
            return "(" + key + ", " + value.toString() + ")";
        }
    }

    private class UpdateWrapper {
        String key;
        Object value;
	long operation_num;
	long start_time;
        public UpdateWrapper(String key, Object value, long operation_num, long start_time) {
            this.key = key;
            this.value = value;
	    this.operation_num = operation_num;
	    this.start_time = start_time;
        }
    }

    private class Update extends Find {

        ArrayList<UpdateWrapper> updates;

        public Update(Label myLabel, String findKey, Object findValue, String newKey, Object newValue) {
            super(myLabel, findKey, findValue);
            this.updates = new ArrayList<>();
            this.updates.add(new UpdateWrapper(newKey, newValue, super.operationNum, super.startTime));
        }

    }

    private class QueryDelete extends Find {
        public QueryDelete(Label myLabel, String key, Object value) {
            super(myLabel, key, value);
        }
    }

    private class GetFollowers extends Find {
        public GetFollowers(Label myLabel, String key, Object value) {
            super(myLabel, key, value);
        }
    }

    private class GetFollowees extends Find {
        public GetFollowees(Label myLabel, String key, Object value) {
            super(myLabel, key, value);
        }
    }

    private class GetPushes extends Find {
	public GetPushes(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }
    
    private class GetPulls extends Find {
	public GetPulls(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }

    private class GetForks extends Find {
	public GetForks(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }

    private class GetIssueComments extends Find {
	public GetIssueComments(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }

    private class GetPosts extends Find {
	public GetPosts(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }

    private class GetTags extends Find {
	public GetTags(Label myLabel, String key, Object value) {
	    super(myLabel, key, value);
	}
    }

        private class AddWithRelationship extends Find {
        Label myLabel;
        String my_key;
        Object my_prop;
        Direction dir;
        public AddWithRelationship(final Label myLabel, final String my_key, final Object my_prop,
                                   final Label rel_label, final String rel_key, final Object rel_val, final Direction dir) {
            super(rel_label, rel_key, rel_val);
            this.myLabel = myLabel;
            this.my_key = my_key;
            this.my_prop = my_prop;
            this.dir = dir;
        }
    }
    
    private class BatchedFind extends Operation {

        ArrayList<Find> finds;
        ResourceIterator<Node> iterator;
        ReentrantLock lock;


	long created_at;
	volatile boolean hasWrites = false;

        public BatchedFind(Find find, ResourceIterator<Node> iterator) {
            super(-1);
            this.finds = new ArrayList<Find>() {
                {
                    add(find);
                }
            };
            this.iterator = iterator;
            this.lock = new ReentrantLock();
	    this.created_at = System.currentTimeMillis();
        }

        public BatchedFind(ArrayList<Find> finds, ResourceIterator<Node> iterator) {
            super(-1);
            this.finds = finds;
            this.iterator = iterator;
            this.lock = new ReentrantLock();
	    this.created_at = System.currentTimeMillis();
        }

        @Override
        boolean isBatchedFind() {
            return true;
        }

        @Override
        void handleRemove() {

        }

        @Override
        public boolean isSpliced() {

            return false;
        }

        @Override
        public String toString() {
            return finds.toString();
        }
    }


    // Results and splicing are not really considered in this implementation
    private class FindResult {
        Label myLabel;
        String key;
        Object value;
        long operationNumber;
        long nodeId;
        int currentIndex;

        // TAG: OPTIME
        long starttime = 0;
        long endtime = 0;

        public FindResult(Label myLabel, String key, Object value, long operationNumber, long nodeId, int currentIndex) {
            this.myLabel = myLabel;
            this.key = key;
            this.value = value;
            this.operationNumber = operationNumber;
            this.nodeId = nodeId;
            this.currentIndex = currentIndex;
        }
    }

    private void resolveSplicing(int index, long nodeId) {
        if(serverOperations.at(index).getClass() != Find.class) {
            return;
        }
        Find splicedFind = (Find)serverOperations.at(index);
        Node node = newNodeProxy(nodeId);
        splicedFind.value = node.getProperty(((Splice)splicedFind.value).desiredProp);
        splicedFind.spliced = false;
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(splicedFind);
        ResourceIterator<Node> it = createBatchedIterator(finds, splicedFind.myLabel.name());
        BatchedFind batchedFind = new BatchedFind(splicedFind, it);
        serverOperations.set(index, batchedFind);

    }


    /* ========================= ARRAY CLASSES ========================= */

    // Double ended array class used for operations array
    // Not robust but good enough to get the job done
    private abstract class DoubleEndedArray<T> {
        final int DEFAULT_SIZE = 16384;
        T[] data;
        int oldest, newest, size, cap;
        ReentrantLock lock;

        public DoubleEndedArray() {
            this.data = null;
            this.oldest = 0;
            this.newest = 0;
            this.size = 0;
            this.cap = DEFAULT_SIZE;
            this.lock = new ReentrantLock();
        }

        public void add(T obj) {
            if (this.size == this.cap) {
                this.resize();
            }
            this.data[this.newest] = obj;
            this.newest = (this.newest + 1) % this.cap;
            this.size++;
            this.moveTail();
        }
        public int size() {
            return this.size;
        }
        public T at(int index) {
            return this.data[index];
        }
        public int indexOf(T obj) {
            for (int i = this.oldest; i != this.newest; i = ((i + 1) % this.cap)) {
                if (this.data[i] != null && this.data[i] == obj) {
                    return i;
                }
            }
            return -1;
        }
        public void remove(int index) {
            this.data[index] = null;
            this.size--;
            this.moveTail();
        }
        public int remove(T obj) {
            for (int i = this.oldest; i != this.newest; i = ((i + 1) % this.cap)) {
                if (this.data[i] != null && this.data[i] == obj) {
                    this.remove(i);
                    return i;
                }
            }
            return -1;
        }

        abstract void resize();
        abstract void moveTail();
        void reset() {
            this.data = null;
            this.oldest = 0;
            this.newest = 0;
            this.size = 0;
            this.cap = DEFAULT_SIZE;
        }
    }

    private double triangularDistribution(double a, double b, double c) {
        double F = (c - a) / (b - a);
        double rand = Math.random();
        if (rand < F) {
            return a + Math.sqrt(rand * (b - a) * (c - a));
        } else {
            return b - Math.sqrt((1 - rand) * (b - a) * (b - c));
        }
    }

    
    // Holds operations to be processed
    private class ServerOperationsArray extends DoubleEndedArray<Operation> {

        public ServerOperationsArray() {
            super();
            this.data = new Operation[this.DEFAULT_SIZE];
        }

        @Override
	public void add(Operation op) {
	    op.operationsArrayIndex = this.newest;
	    super.add(op);
	}
	
        @Override
        void moveTail() {
            while (this.size != 0) {
                if (this.data[this.oldest] == null) {
                    this.oldest = (this.oldest + 1) % this.cap;
                } else if (this.data[this.oldest].getClass() != BatchedFind.class) {
                    this.data[this.oldest].handleRemove();
                    this.data[this.oldest] = null;
                    this.size--;
                    this.oldest = (this.oldest + 1) % this.cap;
                } else {
                    break;
                }
            }
        }

        @Override
        void reset() {
            super.reset();
            this.data = new Operation[this.DEFAULT_SIZE];
        }

        @Override
        void resize() {
            Operation[] newData = new Operation[this.size*2];
            for(int i = oldest; i < newest; i++) {
                newData[i] = this.data[i];
            }
            this.data = newData;
            this.cap *= 2;
        }

        void set(int index, Operation op) {
            if(this.data[index] == null) {
                this.size++;
            }
            if(this.oldest > index) {
                this.oldest = index;
            }
            if(this.newest < index) {
                this.newest = index;
            }
            this.data[index] = op;
	    op.operationsArrayIndex = index;
        }

        public BatchedFind getFirst() {
            for (int i = oldest; i != newest; i = ((i + 1) % this.cap)) {
                //		this.lock.lock();
                try {
                    if (this.data[i] != null && !this.data[i].isSpliced() && this.data[i].isBatchedFind() && ((BatchedFind)this.data[i]).lock.tryLock()) {
                        // this.lock.unlock();
                        return (BatchedFind)this.data[i];
                    }
                } catch (NullPointerException n) {
                    return null;
                }
                //this.lock.unlock();
            }
            return null;
        }
	
        public BatchedFind getLast() {
            for (int i = this.newest; i >= this.oldest; i--) {
                try {
                    if (this.data[i] != null && !this.data[i].isSpliced() && this.data[i].isBatchedFind() && ((BatchedFind)this.data[i]).lock.tryLock()) {
                        return (BatchedFind)this.data[i];
                    }
                } catch (NullPointerException n) {
                    return null;
                }
            }
            return null;
        }

        public BatchedFind getRandom() {
            if (this.size() == 0) {
                return null;
            }
	    for(int retry = 0; retry < 5; retry++) {
		try {
		    int i = (int)(Math.random() * this.size) + this.oldest;
                    if (this.data[i] != null && !this.data[i].isSpliced() && this.data[i].isBatchedFind() && ((BatchedFind)this.data[i]).lock.tryLock()) {
                        return (BatchedFind)this.data[i];
                    }
                } catch (NullPointerException n) {
                    continue;
                }
     
	    }
	    if(Math.random() > 0.5) {
		return this.getLast();
	    }
	    else {
		return this.getFirst();
	    }
        }
    }

    // Holds results that have been served to the client already
    private class ClientResultsArray extends DoubleEndedArray<FindResult> {

        public ClientResultsArray() {
            super();
            this.data = new FindResult[this.DEFAULT_SIZE];
        }

        @Override
        void resize() {
            FindResult[] newData = new FindResult[this.size*2];
            for(int i = oldest; i < newest; i++) {
                newData[i] = this.data[i];
            }
            this.data = newData;
            this.cap *= 2;
        }

        @Override
        void reset() {
            super.reset();
            this.data = new FindResult[this.DEFAULT_SIZE];
        }

        @Override
        void moveTail() {
            while (this.size != 0) {
                if (this.data[this.oldest] == null) {
                    this.oldest++;
                } else {
                    break;
                }
            }
        }

        public Node get(final long operationNum) {
            for(int i = this.oldest; i != this.newest; i = (i+1) % this.cap) {
                if (this.data[i] != null) {
                    if (this.data[i].operationNumber == operationNum) {
                        long nodeId = this.data[i].nodeId;
                        this.data[i] = null;
                        this.size--;
                        this.moveTail();
                        return newNodeProxy(nodeId);
                    }
                }
            }
            return null;
        }

        public Node get(final Label myLabel, final String key, final Object value) {
            for(int i = this.oldest; i != this.newest; i = (i+1) % this.cap) {
                if (this.data[i] != null) {
                    if (this.data[i].myLabel == myLabel
                            && this.data[i].key.equals(key)
                            && this.data[i].value.equals(value)) {
                        long nodeId = this.data[i].nodeId;
                        this.data[i] = null;
                        this.size--;
                        this.moveTail();
                        return newNodeProxy(nodeId);
                    }
                }
            }
            return null;
        }

    }


    // Would hold results that have been found, but not yet served to client
    // (being used for splicing, which isn't working in this implementation)
    private class ServerResultsArray extends DoubleEndedArray<FindResult> {

        public ServerResultsArray() {
            super();
            this.data = new FindResult[this.DEFAULT_SIZE];
        }

        @Override
        void reset() {
            super.reset();
            this.data = new FindResult[this.DEFAULT_SIZE];
        }

        @Override
        void resize() {
            FindResult[] newData = new FindResult[this.size*2];
            for(int i = oldest; i < newest; i++) {
                newData[i] = this.data[i];
            }
            this.data = newData;
            this.cap *= 2;
        }

        @Override
        void moveTail() {
            while (this.size != 0) {
                if (this.data[this.oldest] == null) {
                    this.oldest++;
                } else {
                    break;
                }
            }
        }

        public FindResult getFirst() {
            for (int i = oldest; i != newest; i = ((i + 1) % this.cap)) {
                if (this.data[i] != null) {
                    return this.data[i];
                }
            }
            return null;
        }

        public FindResult getRandom() {
            if (this.size() == 0) {
                return null;
            }
            int index = (int) (Math.random() * (this.newest - this.oldest)) + this.oldest;
            if (this.at(index) == null) {
                return this.getFirst();
            }
            return this.data[index];
        }

    }




    /* ========================= CLASS VARIABLES ========================= */

    // Bookkeeping
    private HashMap<Long, Long> added = new HashMap<>();
    private HashMap<Long, Long> deleted = new HashMap<>();

    private ServerOperationsArray serverOperations = new ServerOperationsArray();
    private ServerResultsArray serverResults = new ServerResultsArray();
    private ClientResultsArray clientResults = new ClientResultsArray();


    private void tryToFuse(BatchedFind batchedFind) {
        // Can only do UpdateUpdate so far
        HashMap<Object, ArrayList<Update>> updates = new HashMap<>();
        for(Find f : batchedFind.finds) {
            if(f.getClass() == Update.class) {
                Update u = (Update)f;
                if(updates.containsKey(u.value)) {
                    updates.get(u.value).add(u);
                }
                else {
                    ArrayList<Update> a = new ArrayList<>();
                    a.add(u);
                    updates.put(u.value, a);
                }
            }
        }
        // Now, updates contains a mapping from desired value to list of updates with that
        // Check if any can be merged
        for(Object o : updates.keySet()) {
            Update fused = updates.get(o).get(0);
            while(updates.get(o).size() > 1) {
                Update u = updates.get(o).get(1);
                fused.updates.addAll(u.updates);
                ((BatchedNodeLabelPropertyIterator)batchedFind.iterator).removeFromQueries(o);
                updates.get(o).remove(u);
                batchedFind.finds.remove(u);
                if(ENABLE_FUSION_DEBUG_OUTPUT) {
                    System.out.println("===== Fused " + fused.updates.size() + " updates together =====");
                }
            }
        }

    }

    /* ========================= BATCHING ========================= */

    // BATCHING_TOLERANCE * STRIDE_SIZE = Number of nodes willing to sacrifice backwards for batching
    // For the general PLP case, always 1
    private int BATCHING_TOLERANCE = 1;

    private boolean canBeBatched(BatchedNodeLabelPropertyIterator it1, BatchedNodeLabelPropertyIterator it2) {
        return Math.abs(it1.numberOfNodesViewed - it2.numberOfNodesViewed) < (BATCHING_TOLERANCE * NODES_TO_PROP);
    }

    // Actually do the batching between op1 and op2
    // Ops will be batched into whichever is less far
    private BatchedFind doBatching(BatchedFind op1, BatchedFind op2) {

        BatchedFind batched;
        if (((BatchedNodeLabelPropertyIterator)op1.iterator).numberOfNodesViewed < ((BatchedNodeLabelPropertyIterator)op2.iterator).numberOfNodesViewed) {
            batched = op1;
            ((BatchedNodeLabelPropertyIterator)batched.iterator).addQueries(((BatchedNodeLabelPropertyIterator)op2.iterator).queries);
            batched.finds.addAll(op2.finds);
        }
        else {
            batched = op2;
            ((BatchedNodeLabelPropertyIterator)batched.iterator).addQueries(((BatchedNodeLabelPropertyIterator)op1.iterator).queries);
            batched.finds.addAll(op1.finds);
        }
        return batched;
    }

    // Limits for batching
    private int BATCH_LIMIT = 500;
    private long BATCH_TIME_LIMIT = 750; //ms


    // Attempt to do batching with op
    // Should only be called if op is locked by current thread
    private BatchedFind tryToBatch(BatchedFind op) {

	int i = op.operationsArrayIndex;
        BatchedFind currentlyBatching = (BatchedFind)serverOperations.at(i);
        try {

	 
	    while(currentlyBatching.finds.size() < this.BATCH_LIMIT && i < serverOperations.newest) {
		Operation current = serverOperations.at(i);

		if(current == null || current == currentlyBatching || !current.isBatchedFind()) {
		    i++;
                    continue;
                }
		
		// In use
                if( !((BatchedFind)current).lock.tryLock() ) {
		    // On read-only streams, this can also be converted to
		    // i++;
		    // continue;
		    break;
                }
		
		// Cant be batched (too far)
                if( !canBeBatched( (BatchedNodeLabelPropertyIterator)((BatchedFind)current).iterator, (BatchedNodeLabelPropertyIterator)currentlyBatching.iterator )) {
		    break;
		}

		// Batch
		if(this.ENABLE_BATCHING_DEBUG_OUTPUT) {
		    System.out.println("===== Batched " + ( ((BatchedFind)current).finds.size() + currentlyBatching.finds.size() ) + " operations =====");
		}
		BatchedFind batched = doBatching((BatchedFind)current, currentlyBatching);
		batched.lock.lock();

		// Remove both, set at the location of the oldest operation
		serverOperations.lock.lock();
		serverOperations.remove(i);
		int removed_index = serverOperations.remove(currentlyBatching);
		serverOperations.set(Math.min(removed_index, i), batched);
		serverOperations.lock.unlock();

		currentlyBatching = batched;

		i++;
            }
        
	    i = currentlyBatching.operationsArrayIndex;
	    

	    while(currentlyBatching.finds.size() < this.BATCH_LIMIT && i >= serverOperations.oldest) {
		Operation current = serverOperations.at(i);
		
		if(current == null || current == currentlyBatching || !current.isBatchedFind()) {
		    i--;
		    continue;
		}
		
		// In use
		if( !((BatchedFind)current).lock.tryLock() ) {
		    // On read-only streams, this can also be converted to
		    // i--;
		    // continue;
		     
		    break;
		}
	    
		// Cant be batched (too far)
		if( !canBeBatched( (BatchedNodeLabelPropertyIterator)((BatchedFind)current).iterator, (BatchedNodeLabelPropertyIterator)currentlyBatching.iterator )) {
		    break;
		}
	    
	        // Batch
	        if(this.ENABLE_BATCHING_DEBUG_OUTPUT) {
		    System.out.println("===== Batched " + ( ((BatchedFind)current).finds.size() + currentlyBatching.finds.size() ) + " operations =====");
	        }
	        BatchedFind batched = doBatching((BatchedFind)current, currentlyBatching);
	        batched.lock.lock();
	        // Remove both, set at the location of the oldest operation
	        serverOperations.lock.lock();
	        serverOperations.remove(i);
	        int removed_index = serverOperations.remove(currentlyBatching);
	        serverOperations.set(Math.min(removed_index, i), batched);
	        serverOperations.lock.unlock();
	        currentlyBatching = batched;
	    
	        i--;
	    }

        } catch (Exception e) {
	    System.out.println("Program execution cancelled: Recieved exception in tryToBatch");
	    System.exit(-1);
        }
    
    
        tryToFuse(currentlyBatching);

	return currentlyBatching;
    }


    private ResourceIterator<Node> createBatchedIterator(ArrayList<Find> finds, String label) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        Statement statement = transaction.acquireStatement();

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor();
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor();
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor();
        int labelId = transaction.tokenRead().nodeLabel(label);

        transaction.dataRead().nodeLabelScan(labelId, nodeLabelCursor);

        IndexQuery[] queries = new IndexQuery[finds.size()];
        for (int i = 0; i < queries.length; i++) {
            int propertyId = transaction.tokenRead().propertyKey(finds.get(i).key);
            queries[i] = IndexQuery.exact(propertyId, Values.of(finds.get(i).value));
        }

        return new BatchedNodeLabelPropertyIterator(transaction.dataRead(),
                nodeLabelCursor,
                nodeCursor,
                propertyCursor,
                statement,
                this::newNodeProxy,
                queries);
    }


    // Actually do propagation of prop
    // Check for completed ops and handle
    private boolean doBatchedPropagation(BatchedFind prop) {
        if(prop == null) {
            return false;
        }
	
	prop = this.tryToBatch(prop);	
	
        BatchedNodeLabelPropertyIterator it = (BatchedNodeLabelPropertyIterator)prop.iterator;
        HashMap<Object, Long> ret = it.get();

        if(ret.isEmpty()) {
            if(!it.hasNext()) {
                this.serverOperations.lock.lock();
                serverOperations.remove(prop);
                this.serverOperations.lock.unlock();
                throw new NoSuchElementException("Queries " + prop.finds.toString() + " not found");

            }
            return false;
        }

        ArrayList<Find> found = new ArrayList<Find>();

        for(Find f : prop.finds) {
            if(ret.containsKey(f.value)) {
                Long returned_nodeid = ret.get(f.value);

                // If node was added, and has operation number > the node we're propagating, the node should not be found
                if(added.containsKey(returned_nodeid) && added.get(returned_nodeid) > f.operationNum) {
                    throw new NoSuchElementException();
                }

                // If node was deleted, and has operation number < the node we're propagating, the node should not be found
                if(deleted.containsKey(returned_nodeid) && deleted.get(returned_nodeid) < f.operationNum) {
                    throw new NoSuchElementException();
                }

                if(f.getClass() == Update.class) {
                    long stop = System.currentTimeMillis();
                    try (Transaction tx = beginTx()) {
                        Node n = newNodeProxy(returned_nodeid);
                        Update u = (Update)f;
                        for(UpdateWrapper kv : u.updates) {
                            n.setProperty(kv.key, kv.value);
			    opTimeWrapper.op(kv.operation_num, System.currentTimeMillis() - kv.start_time);
                        }
                        stop = System.currentTimeMillis();
                        if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                            System.out.println("===== UPDATE WITH " + u.updates.size() + " COMPLETED =====" );
                        }
                        tx.success();
                    } catch (Exception e) {
                        System.out.println("Error in update: " + e );
			System.exit(-1);
                    }
                    found.add(f);
                    continue;
                }

                if(f.getClass() == QueryDelete.class) {
                    long stop = System.currentTimeMillis();
		    opTimeWrapper.op(f.operationNum, stop - f.startTime);
		    try (Transaction tx = beginTx()) {
                        Node n = newNodeProxy(returned_nodeid);
			n.setProperty("deleted", true);
                        stop = System.currentTimeMillis();
                        tx.success();
                    } catch (Exception e) {
                        System.out.println("Error in delete: " + e );			
                    }

                    this.deleted.put(returned_nodeid, f.operationNum);
                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== QUERIED DELETE COMPLETED =====");
                    }
                    continue;
                }
		
                if(f.getClass() == GetFollowers.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(Direction.INCOMING).iterator();
                        Set<Relationship> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next());
                        }			
                        stop = System.currentTimeMillis();
                        size = rels_set.size();
			tx.success();
                    } catch (Exception e) {
                        System.out.println("Error in getFollowers: " + e );
                        System.exit(-1);
                    }


		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETFOLLOWERS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }
		    
		    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET FOLLOWERS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }
                if(f.getClass() == GetFollowees.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(Direction.OUTGOING).iterator();
                        Set<Relationship> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next());
                        }
                        stop = System.currentTimeMillis();
                        size = rels_set.size();			
                    } catch (Exception e) {
                        System.out.println("Error in getFollowees: " + e );
                        System.exit(-1);			
                    }

		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETFOLLOWERS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }
		    
                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET FOLLOWEES COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }

                }

		if(f.getClass() == GetPushes.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("PUSHED_TO")).iterator();
			Set<Node> node_set = new HashSet<>();
                        while(rels.hasNext()) {
                            node_set.add(rels.next().getOtherNode(n));
                        }
                        stop = System.currentTimeMillis();
                        size = node_set.size();
		    } catch (Exception e) {
                        System.out.println("Error in getPushes: " + e );
                        System.exit(-1);			
                    }
		   
		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETPUSHES");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET PUSHES COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }

		if(f.getClass() == GetPulls.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("PULLED_FROM")).iterator();
                        Set<Node> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next().getOtherNode(n));
                        }
                        stop = System.currentTimeMillis();
                        size = rels_set.size();
                    } catch (Exception e) {
                        System.out.println("Error in getPulls: " + e );
                        System.exit(-1);			
                    }


		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETPULLS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET PULLS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }

		if(f.getClass() == GetForks.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("FORKED_FROM")).iterator();
                        Set<Node> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next().getOtherNode(n));
                        }
                        stop = System.currentTimeMillis();
                        size = rels_set.size();
                    } catch (Exception e) {
                        System.out.println("Error in getForks: " + e );
                        System.exit(-1);			
                    }

		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETFORKS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET FORKS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }
		if(f.getClass() == GetIssueComments.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("COMMENTED_ON")).iterator();
                        Set<Node> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next().getOtherNode(n));
                        }
                        stop = System.currentTimeMillis();
                        size = rels_set.size();
                    } catch (Exception e) {
                        System.out.println("Error in getIssueComments: " + e );
                        System.exit(-1);			
                    }

		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR GETISSUECOMMENTS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET ISSUE COMMENTS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }
		
		if(f.getClass() == GetPosts.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("POSTED")).iterator();
                        Set<Node> rels_set = new HashSet<>();
                        while(rels.hasNext()) {
                            rels_set.add(rels.next().getOtherNode(n));
                        }
                        stop = System.currentTimeMillis();
                        size = rels_set.size();
                    } catch (Exception e) {
                        System.out.println("Error in getPosts: " + e );
                        System.exit(-1);			
                    }

		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR POSTS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET POSTS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }


		if(f.getClass() == GetTags.class) {
                    long stop = 0;
                    int size = -1;
                    try (Transaction tx = beginTx()) {
                        NodeProxy n = newNodeProxy(returned_nodeid);
                        ResourceIterator<Relationship> rels = n.getRelationships(RelationshipType.withName("POSTED")).iterator();
			Set<Node> tags_set = new HashSet<>();
                        while(rels.hasNext()) {
			    Node n2 = rels.next().getOtherNode(n);
			    Iterator<Relationship> rels2 = n2.getRelationships(RelationshipType.withName("HAS_TAG")).iterator();
			    while(rels2.hasNext()) {
				tags_set.add(rels2.next().getOtherNode(n2));
			    }
                        }
                        stop = System.currentTimeMillis();
                        size = tags_set.size();
                    } catch (Exception e) {
                        System.out.println("Error in getTags: " + e );
                        System.exit(-1);			
                    }

		    if(!finishedTimes.containsKey(f.value)) {
			    System.out.println("MLP TIME ERROR TAGS");
			    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		    }
		    else {
			opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		    }
                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== GET TAGS COMPLETED WITH " + size + " RELATIONSHIPS =====");
                    }
                }
	       		
                if(f.getClass() == AddWithRelationship.class) {
                    long stop = System.currentTimeMillis();
		    opTimeWrapper.op(f.operationNum, stop - f.startTime);
		    try (Transaction tx = beginTx()) {
                        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
                        try (Statement ignore = transaction.acquireStatement()) {
                            TokenWrite tokenWrite = transaction.tokenWrite();

                            int[] labelIds = new int[1];
                            String[] labelNames = new String[1];
                            labelNames[0] = ((AddWithRelationship) f).myLabel.toString();
                            tokenWrite.labelGetOrCreateForNames(labelNames, labelIds);

                            Write write = transaction.dataWrite();
                            long nodeId = write.nodeCreateWithLabels(labelIds);

                            added.put(nodeId, f.operationNum);
                            Node n = newNodeProxy(nodeId);
                            Node returned = newNodeProxy(returned_nodeid);

                            n.setProperty(((AddWithRelationship) f).my_key, ((AddWithRelationship) f).my_prop);

                            // Hard code this for now
			    final RelationshipType relType = RelationshipType.withName("COMMENTED_ON_POST_OF");
			    
                            Direction dir = ((AddWithRelationship) f).dir;
                            if(dir == Direction.BOTH || dir == Direction.INCOMING) {
                                returned.createRelationshipTo(n, relType);
                            }
                            if(dir == Direction.BOTH || dir == Direction.OUTGOING) {
                                n.createRelationshipTo(returned, relType);
                            }
                            stop = System.currentTimeMillis();

                        } catch (ConstraintValidationException e) {
                            throw new ConstraintViolationException("Unable to add label.", e);
                        } catch (SchemaKernelException e) {
                            throw new IllegalArgumentException(e);
                        } catch (KernelException e) {
                            throw new ConstraintViolationException(e.getMessage(), e);
                        }
                        tx.success();
                    } catch (Exception e) {
                        System.out.println("Error in addWithRelationship: " + e );
			System.exit(-1);
                    }

                    found.add(f);
                    if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                        System.out.println("===== ADD WITH RELATIONSHIP COMPLETED =====");
                    }
                    continue;

                }

                int index = serverOperations.newest;

                if(this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                    System.out.println("================ found #" + f.operationNum + " in thread " + Thread.currentThread().getId() + " ================" );
                }
                // TAG: OPTIME
                this.createResult(f.myLabel, f.key, f.value, f.operationNum, returned_nodeid, index, f.startTime);

		if(!finishedTimes.containsKey(f.value)) {
		    opTimeWrapper.op(f.operationNum, System.currentTimeMillis() - f.startTime);
		}
		else {
		    opTimeWrapper.op(f.operationNum, finishedTimes.get(f.value) - f.startTime);
		}
		
                found.add(f);
            }
        }

	prop.finds.removeAll(found);

	// Batch completed
        if(prop.finds.size() == 0) {
            this.serverOperations.lock.lock();	
            serverOperations.remove(prop);
            this.serverOperations.lock.unlock();
        }

        return true;
    }


    /* ========================= OTHER ========================= */


    // Add result to serverResults list, to be backwards propagated to spliced finds
    // TAG: OPTIME
    private void createResult(final Label myLabel, final String key, final Object value, final long operationNumber, long nodeId, int index, long startTime) {
        FindResult fr = new FindResult(myLabel, key, value, operationNumber, nodeId, index);
        // TAG: OPTIME
        fr.starttime = startTime;
        this.serverResults.lock.lock();
        this.serverResults.add(fr);
        this.serverResults.lock.unlock();
    }


    /* ========================= PUBLIC LAZY INTERFACE ========================= */


    public Node newCreateNode(long opNum, Label... labels) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        try (Statement ignore = transaction.acquireStatement()) {
            TokenWrite tokenWrite = transaction.tokenWrite();
            int[] labelIds = new int[labels.length];
            String[] labelNames = new String[labels.length];
            for (int i = 0; i < labelNames.length; i++) {
                labelNames[i] = labels[i].name();
            }
            tokenWrite.labelGetOrCreateForNames(labelNames, labelIds);

            Write write = transaction.dataWrite();
            long nodeId = write.nodeCreateWithLabels(labelIds);

            added.put(nodeId, opNum);

            return newNodeProxy(nodeId);
        } catch (ConstraintValidationException e) {
            throw new ConstraintViolationException("Unable to add label.", e);
        } catch (SchemaKernelException e) {
            throw new IllegalArgumentException(e);
        } catch (KernelException e) {
            throw new ConstraintViolationException(e.getMessage(), e);
        }
    }

        // Create a new node with relationship to second
    public long createNodeWithRelationship(final Label myLabel, final String my_key, final Object my_prop,
                                           final Label rel_label, final String rel_key, final Object rel_val, final Direction dir) {
        AddWithRelationship rel = new AddWithRelationship(myLabel, my_key, my_prop, rel_label, rel_key, rel_val, dir);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(rel);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(rel, it);
        this.serverOperations.lock.lock();
        this.serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return rel.operationNum;
    }

    
    public long delete(long id) {
        Delete del = new Delete(id);
        this.deleted.put(id, del.operationNum);
        this.serverOperations.lock.lock();
        this.serverOperations.add(del);
        this.serverOperations.lock.unlock();
        return del.operationNum;
    }
    
    public long getFollowers(final Label myLabel, final String key, final Object value) {
        GetFollowers gf = new GetFollowers(myLabel, key, value);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(gf);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(gf, it);
        this.serverOperations.lock.lock();
        serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return gf.operationNum;
    }
    
    public long getFollowees(final Label myLabel, final String key, final Object value) {
        GetFollowees gf = new GetFollowees(myLabel, key, value);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(gf);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(gf, it);
        this.serverOperations.lock.lock();
        serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return gf.operationNum;
    }

    public long getPushes(final Label myLabel, final String key, final Object value) {
	GetPushes gp = new GetPushes(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gp);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gp, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gp.operationNum;
    }

    public long getPulls(final Label myLabel, final String key, final Object value) {
	GetPulls gp = new GetPulls(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gp);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gp, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gp.operationNum;
    }

    public long getPosts(final Label myLabel, final String key, final Object value) {
	GetPosts gp = new GetPosts(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gp);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gp, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gp.operationNum;
    }

    public long getForks(final Label myLabel, final String key, final Object value) {
	GetForks gf = new GetForks(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gf);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gf, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gf.operationNum;
    }

    public long getIssueComments(final Label myLabel, final String key, final Object value) {
	GetIssueComments gc = new GetIssueComments(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gc);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gc, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gc.operationNum;
    }

    public long getTags(final Label myLabel, final String key, final Object value) {
	GetTags gt = new GetTags(myLabel, key, value);
	ArrayList<Find> finds = new ArrayList<Find>();
	finds.add(gt);
	ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
	BatchedFind batchedFind = new BatchedFind(gt, it);
	this.serverOperations.lock.lock();
	serverOperations.add(batchedFind);
	this.serverOperations.lock.unlock();
	return gt.operationNum;
    }  
    
    public long lazyFindNodesSpliced(final Label myLabel, final String key, final long operationNum, final String desiredProp) {
        Find f = new Find(myLabel, key, new Splice(operationNum, desiredProp));
        this.serverOperations.lock.lock();
        serverOperations.add(f);
        this.serverOperations.lock.unlock();
        return f.operationNum;
    }

    public long lazyFindNodes(final Label myLabel, final String key, final Object value) {
        Find find = new Find(myLabel, key, value);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(find);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(find, it);
        this.serverOperations.lock.lock();
        serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return find.operationNum;
    }

    public long update(final Label myLabel, final String findKey, final Object findValue, final String newKey, final Object newValue) {
        Update update = new Update(myLabel, findKey, findValue, newKey, newValue);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(update);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(update, it);
        this.serverOperations.lock.lock();
        serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return update.operationNum;
    }

    public long queryDelete(final Label myLabel, final String key, final Object value) {
        QueryDelete delete = new QueryDelete(myLabel, key, value);
        ArrayList<Find> finds = new ArrayList<Find>();
        finds.add(delete);
        ResourceIterator<Node> it = createBatchedIterator(finds, myLabel.name());
        BatchedFind batchedFind = new BatchedFind(delete, it);
        this.serverOperations.lock.lock();
        serverOperations.add(batchedFind);
        this.serverOperations.lock.unlock();
        return delete.operationNum;
    }

    private boolean doPropagate() {
        BatchedFind to_propagate = serverOperations.getRandom();
        if(to_propagate == null) {
            return false;
        }
        this.doBatchedPropagation(to_propagate);
        to_propagate.lock.unlock();
	return true;
    }

    /* ========================= MULTITHREADING ========================= */

    ExecutorService thread_pool = Executors.newFixedThreadPool(NUM_THREADS);
        private void cyclePropagate() {
        while (this.INPUT_LEFT || this.numOperationsRemaining() != 0) {
	    boolean did_work = doPropagate();
	    if(!this.INPUT_LEFT && !did_work) {
		break;
	    }
        }	
    }

    private class PropagationRunner implements Runnable {
        public void run() {
            cyclePropagate();
        }
    }

    private boolean INPUT_LEFT = false;

    public void beginPropagation() {
        System.out.println(" ===== Beginning propagation with " + this.NUM_THREADS + " threads =====");
        this.INPUT_LEFT = true;
        for (int i = 0; i < this.NUM_THREADS; i++) {
            if (this.ENABLE_OPERATION_DEBUG_OUTPUT) {
                System.out.println(" ===== Beginning thread " + i + " ===== ");
            }
            this.thread_pool.execute(new PropagationRunner());
        }
    }

    public void stopPropagation() {
	System.out.println(" ===== Recieved signal to stop accepting input ===== ");
	this.INPUT_LEFT = false;
	System.out.println(" ===== Waiting for remaining operations to finish ===== ");
	long last_printed = -1;
       	while(this.numOperationsRemaining() != 0) {
	    long num = this.numOperationsRemaining();
	    if((num % 50 == 0 || num < 25) && num != last_printed) {
		System.out.println(" ===== Remaining to process: " + num + " =====");
		last_printed = num;
	    }
	}
	System.out.println(" ===== All remaining operations finished, exiting ===== ");
        this.thread_pool.shutdown();
    }


    public void propagate() {
        this.thread_pool.execute(new PropagationRunner());
    }

    public Node getResult(final Label myLabel, final String key, final Object value) {
        return clientResults.get(myLabel, key, value);
    }

    public Node getResult(long operationNum) {
        return clientResults.get(operationNum);
    }


    // Not functional at this point
    public boolean propagateResults() {

        // Get random result from serverResults array
        FindResult random = this.serverResults.getRandom();

        if(random == null) {
            return false;
        }

        // Get index that result has currently propagated to, and check backward up to NODES_TO_PROP steps
        for(int i = 0; i < NODES_TO_PROP; i++) {
            int index = random.currentIndex;
            if(index < serverOperations.oldest) {
                // If reach the oldest operation, move result to postprop and remove from serverResults

                // TAG: OPTIME
                random.endtime = System.currentTimeMillis();
                opTimeWrapper.op(random.operationNumber, (random.endtime - random.starttime));

                this.clientResults.lock.lock();
                this.clientResults.add(random);
                this.clientResults.lock.unlock();
                this.serverResults.lock.lock();
                this.serverResults.remove(random);
                this.serverResults.lock.unlock();
                return true;
            }
            // If a spliced find is found with the same properties, replace it with a regular find with actual value
            if(serverOperations.at(index) != null
                    && serverOperations.at(index).isSpliced()
                    && ((Splice)((Find) serverOperations.at(index)).value).operationNumber == random.operationNumber) {
                resolveSplicing(index, random.nodeId);
            }
            random.currentIndex--;

        }

        return false;

    }


    /* ========================= PUBLIC BUT NOT PART OF INTERFACE ========================= */

    // Parse line for the lazy system
    public void parseLine(final Label myLabel, String str) {
        Scanner line = new Scanner(str);
        line.useDelimiter(" ");
        String op = line.next();
        long start;
        switch (op) {
            case "add":
                String add_key = line.next();
                int add_val = Integer.parseInt(line.next());
                String add_rel_key = line.next();
                int add_rel_val = Integer.parseInt(line.next());
                Direction dir;
                switch(line.next()) {
                    case "INCOMING":
                        dir = Direction.INCOMING;
                        break;
                    case "OUTGOING":
                        dir = Direction.OUTGOING;
                        break;
                    default:
                        dir = Direction.BOTH;
                        break;
                }
                this.createNodeWithRelationship(myLabel, add_key, add_val, myLabel, add_rel_key, add_rel_val, dir);
                break;
            case "delete":
                String k_to_delete = line.next();
                int k_d = Integer.parseInt(k_to_delete);
                this.queryDelete(myLabel, "userId", k_d);
                break;
            case "find":
                String k_to_find = line.next();
                boolean splice = k_to_find.charAt(0) == 'x';
                if (splice) {
                    k_to_find = k_to_find.substring(1);
                }
                int k_f = Integer.parseInt(k_to_find);
                if (splice) {
                    this.lazyFindNodesSpliced(myLabel, "userId", k_f, "value");
                } else {
                    this.lazyFindNodes(myLabel, "userId", k_f);
                }
                break;
            case "getFollowers":
                int k_f_followers = Integer.parseInt(line.next());
                this.getFollowers(myLabel, "userId", k_f_followers);
                break;
            case "getFollowees":
                int k_f_followees = Integer.parseInt(line.next());
                this.getFollowees(myLabel, "userId", k_f_followees);
                break;
	    case "getPushes":
		int k_f_pushes = Integer.parseInt(line.next());
		this.getPushes(myLabel, "userId", k_f_pushes);
		break;
	    case "getPulls":
		int k_f_pulls = Integer.parseInt(line.next());
		this.getPulls(myLabel, "userId", k_f_pulls);
		break;
	    case "getForks":
		int k_f_forks = Integer.parseInt(line.next());
		this.getForks(myLabel, "userId", k_f_forks);
		break;
	    case "getIssueComments":
		int k_f_comments = Integer.parseInt(line.next());
		this.getIssueComments(myLabel, "userId", k_f_comments);
		break;
	    case "getPosts":
	        int k_f_posts = Integer.parseInt(line.next());
		this.getPosts(myLabel, "userId", k_f_posts);
		break;
	    case "getTags":
	        int k_f_tags = Integer.parseInt(line.next());
	        this.getTags(myLabel, "userId", k_f_tags);
	        break;
	    case "update":
                String findKey = line.next();
                int findVal = Integer.parseInt(line.next());
                String newKey = line.next();
                int newVal = Integer.parseInt(line.next());
                this.update(myLabel, findKey, findVal, newKey, newVal);
                break;
	    default:
	        System.out.println("Error: unknown operation " + str);
        }
    }

    // Parse line for the eager system
    public void parseLineEager(final Label myLabel, String str, long eagerOpNum, long wait_time) {
        Scanner line = new Scanner(str);
        line.useDelimiter(" ");
        String op = line.next();
        long start;
        long stop;
        switch (op) {
            case "add":
                String arg1 = line.next();
                start = System.currentTimeMillis();
                Node n = this.createNode(myLabel);
                n.setProperty("userId", Integer.parseInt(arg1));
                this.opTimeWrapper.op(eagerOpNum, System.currentTimeMillis() - start);
                break;
            case "delete":
                String k_to_delete = line.next();
                int k_d = Integer.parseInt(k_to_delete);
                start = System.currentTimeMillis();
                long startdel = System.currentTimeMillis();
                Node nod = findNodesTimed(myLabel, "userId", k_d).next();
                Iterable<Relationship> rels = nod.getRelationships();
                rels.forEach(r -> r.delete());
                nod.delete();
                this.opTimeWrapper.op(eagerOpNum, System.currentTimeMillis() - start);

                break;
            case "find":
                String k_to_find = line.next();
                boolean splice = k_to_find.charAt(0) == 'x';
                if (splice) {
                    break;
                }
                int k_f = Integer.parseInt(k_to_find);
                start = System.currentTimeMillis();
                this.findNodesTimed(myLabel, "userId", k_f).next();
                this.opTimeWrapper.op(eagerOpNum, System.currentTimeMillis() - start);
                break;
            case "getFollowers":
                int k_f_followers = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_followers = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_followers).next().getId());
                ResourceIterator<Relationship> followersIterator = n_followers.getRelationships(Direction.INCOMING).iterator();
                Set<Relationship> followersSet = new HashSet<>();
                while(followersIterator.hasNext()) {
                    followersSet.add(followersIterator.next());
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
            case "getFollowees":
                int k_f_followees = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_followees = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_followees).next().getId());
                ResourceIterator<Relationship> followeesIterator = n_followees.getRelationships(Direction.OUTGOING).iterator();
                Set<Relationship> followeesSet = new HashSet<>();
                while(followeesIterator.hasNext()) {
                    followeesSet.add(followeesIterator.next());
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
            case "getPushes":
                int k_f_pushes = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_pushes = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_pushes).next().getId());
                ResourceIterator<Relationship> pushesIterator = n_pushes.getRelationships(RelationshipType.withName("PUSHED_TO")).iterator();
                Set<Node> pushesSet = new HashSet<>();
                while(pushesIterator.hasNext()) {
                    pushesSet.add(pushesIterator.next().getOtherNode(n_pushes));
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
            case "getPulls":
                int k_f_pulls = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_pulls = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_pulls).next().getId());
                ResourceIterator<Relationship> pullsIterator = n_pulls.getRelationships(RelationshipType.withName("PULLED_FROM")).iterator();
                Set<Node> pullsSet = new HashSet<>();
                while(pullsIterator.hasNext()) {
                    pullsSet.add(pullsIterator.next().getOtherNode(n_pulls));
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
     	    case "getForks":
                int k_f_forks = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_forks = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_forks).next().getId());
                ResourceIterator<Relationship> forksIterator = n_forks.getRelationships(RelationshipType.withName("FORKED_FROM")).iterator();
                Set<Node> forksSet = new HashSet<>();
                while(forksIterator.hasNext()) {
                    forksSet.add(forksIterator.next().getOtherNode(n_forks));
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
     	    case "getIssueComments":
                int k_f_comments = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_comments = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_comments).next().getId());
                ResourceIterator<Relationship> commentsIterator = n_comments.getRelationships(RelationshipType.withName("COMMENTED_ON")).iterator();
                Set<Node> commentsSet = new HashSet<>();
                while(commentsIterator.hasNext()) {
                    commentsSet.add(commentsIterator.next().getOtherNode(n_comments));
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;
	    case "getPosts":
	        int k_f_posts = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                NodeProxy n_posts = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_posts).next().getId());
                ResourceIterator<Relationship> postsIterator = n_posts.getRelationships(RelationshipType.withName("POSTED")).iterator();
                Set<Node> postsSet = new HashSet<>();
                while(postsIterator.hasNext()) {
                    postsSet.add(postsIterator.next().getOtherNode(n_posts));
                }
                stop = System.currentTimeMillis();
                this.opTimeWrapper.op(eagerOpNum, stop - start);
                break;	    	       
	    case "getTags":
		int k_f_tags = Integer.parseInt(line.next());
		start = System.currentTimeMillis();
		NodeProxy n_tags = newNodeProxy(this.findNodesTimed(myLabel, "userId", k_f_tags).next().getId());
		ResourceIterator<Relationship> tagsRelsIterator = n_tags.getRelationships(RelationshipType.withName("POSTED")).iterator();
		Set<Node> tags_set = new HashSet<>();
		while(tagsRelsIterator.hasNext()) {
		    Node n_tags2 = tagsRelsIterator.next().getOtherNode(n_tags);
		    Iterator<Relationship> tagsRelsIterator2 = n_tags2.getRelationships(RelationshipType.withName("HAS_TAG")).iterator();
		    while(tagsRelsIterator2.hasNext()) {
			tags_set.add(tagsRelsIterator2.next().getOtherNode(n_tags2));
		    }
		}
		stop = System.currentTimeMillis();
		this.opTimeWrapper.op(eagerOpNum, stop - start);
		break;
	    case "update":
                String findKey = line.next();
                int findObject = Integer.parseInt(line.next());
                String newKey = line.next();
                int newObject = Integer.parseInt(line.next());
                start = System.currentTimeMillis();
                this.findNodesTimed(myLabel, findKey, findObject).next().setProperty(newKey, newObject);
                this.opTimeWrapper.op(eagerOpNum, System.currentTimeMillis() - start);
        }
        this.opTimeWrapper.wait(eagerOpNum, wait_time);
    }

    public boolean reset(final Label myLabel) {
        this.serverOperations.reset();
        this.serverResults.reset();
        this.clientResults.reset();
        this.added.clear();
        this.deleted.clear();
        return true;
    }


    public boolean hasOperationsRemaining() {
        return this.serverOperations.size() > 0 || this.serverResults.size() > 0;
    }

    public int numOperationsRemaining() {
	this.serverOperations.lock.lock();
	int size = this.serverOperations.size();
	this.serverOperations.lock.unlock();
	return size;
    }

    // Operation timing related stuff
    OperationTimeWrapper opTimeWrapper = new OperationTimeWrapper();
    public OperationTimeWrapper getOpTimeWrapper() {
        return this.opTimeWrapper;
    }

    public void op(long opNum, long time) {
        this.opTimeWrapper.op(opNum, time);
    }
    public void wait(long opNum, long time) {
        this.opTimeWrapper.wait(opNum, time);
    }

    
    // These class are used to ensure a more fair baseline for EP
    // It disables prefetching in the eager iterator, as the PLP/MLP implementations do not do prefetching
    private abstract static class NonPrefetchingNodeResourceIterator implements ResourceIterator<Node> {
        private final Statement statement;
        private final NodeFactory nodeFactory;
        private long next;
        private boolean closed;

        private static final long NOT_INITIALIZED = -2L;
        protected static final long NO_ID = -1L;

        NonPrefetchingNodeResourceIterator(Statement statement, NodeFactory nodeFactory) {
            this.statement = statement;
            this.nodeFactory = nodeFactory;
            this.next = NOT_INITIALIZED;
        }

        @Override
        public boolean hasNext() {
            if (next == NOT_INITIALIZED) {
                next = fetchNext();
            }
            return next != NO_ID;
        }

        @Override
        public Node next() {
            if (!hasNext()) {
                close();
                throw new NoSuchElementException();
            }
            return nodeFactory.make(next);
        }

        @Override
        public void close() {
            if (!closed) {
                next = NO_ID;
                closeResources(statement);
                closed = true;
            }
        }

        abstract long fetchNext();

        abstract void closeResources(Statement statement);
    }

    private static class NonPrefetchingNodeLabelPropertyIterator extends NonPrefetchingNodeResourceIterator {
        private final Read read;
        private final NodeLabelIndexCursor nodeLabelCursor;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        private final IndexQuery[] queries;

        NonPrefetchingNodeLabelPropertyIterator(
                Read read,
                NodeLabelIndexCursor nodeLabelCursor,
                NodeCursor nodeCursor,
                PropertyCursor propertyCursor,
                Statement statement,
                NodeFactory nodeFactory,
                IndexQuery... queries) {
            super(statement, nodeFactory);
            this.read = read;
            this.nodeLabelCursor = nodeLabelCursor;
            this.nodeCursor = nodeCursor;
            this.propertyCursor = propertyCursor;
            this.queries = queries;
        }

        @Override
        protected long fetchNext() {
            boolean hasNext;
            do {
                hasNext = nodeLabelCursor.next();

            } while (hasNext && !hasPropertiesWithValues());

            if (hasNext) {
                return nodeLabelCursor.nodeReference();
            } else {
                close();
                return NO_ID;
            }
        }

        @Override
        void closeResources(Statement statement) {
            IOUtils.closeAllSilently(statement, nodeLabelCursor, nodeCursor, propertyCursor);
        }

        private boolean hasPropertiesWithValues() {
            int targetCount = queries.length;
            read.singleNode(nodeLabelCursor.nodeReference(), nodeCursor);
            if (nodeCursor.next()) {
                nodeCursor.properties(propertyCursor);
                while (propertyCursor.next()) {
                    for (IndexQuery query : queries) {
                        if (propertyCursor.propertyKey() == query.propertyKeyId()) {
                            if (query.acceptsValueAt(propertyCursor)) {
                                targetCount--;
                                if (targetCount == 0) {
                                    return true;
                                }
                            } else {
                                return false;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }

    // Find nodes (eager) without prefetching
    public ResourceIterator<Node> findNodesTimed(final Label myLabel, final String key, final Object value) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);
        TokenRead tokenRead = transaction.tokenRead();
        int labelId = tokenRead.nodeLabel(myLabel.name());
        int propertyId = tokenRead.propertyKey(key);
        return nodesByLabelAndPropertyTimed(transaction, labelId, IndexQuery.exact(propertyId, Values.of(value)));
    }
    private ResourceIterator<Node> nodesByLabelAndPropertyTimed(KernelTransaction transaction, int labelId, IndexQuery query) {
        Statement statement = transaction.acquireStatement();
        Read read = transaction.dataRead();

        if (query.propertyKeyId() == TokenRead.NO_TOKEN || labelId == TokenRead.NO_TOKEN) {
            statement.close();
            return emptyResourceIterator();
        }
        IndexReference index = transaction.schemaRead().index(labelId, query.propertyKeyId());
        if (index != IndexReference.NO_INDEX) {
            // Ha! We found an index - let's use it to find matching nodes
            try {
                NodeValueIndexCursor cursor = transaction.cursors().allocateNodeValueIndexCursor();
                read.nodeIndexSeek(index, cursor, IndexOrder.NONE, false, query);

                return new NodeCursorResourceIterator<>(cursor, statement, this::newNodeProxy);
            } catch (KernelException e) {
                // weird at this point but ignore and fallback to a label scan
            }
        }

        return getNodesByLabelAndPropertyWithoutIndexTimed(statement, labelId, query);
    }
    private ResourceIterator<Node> getNodesByLabelAndPropertyWithoutIndexTimed(
            Statement statement, int labelId, IndexQuery... queries) {
        KernelTransaction transaction = statementContext.getKernelTransactionBoundToThisThread(true);

        NodeLabelIndexCursor nodeLabelCursor = transaction.cursors().allocateNodeLabelIndexCursor();
        NodeCursor nodeCursor = transaction.cursors().allocateNodeCursor();
        PropertyCursor propertyCursor = transaction.cursors().allocatePropertyCursor();

        transaction.dataRead().nodeLabelScan(labelId, nodeLabelCursor);

        return new NonPrefetchingNodeLabelPropertyIterator(transaction.dataRead(),
                nodeLabelCursor,
                nodeCursor,
                propertyCursor,
                statement,
                this::newNodeProxy,
                queries);
    }

}
