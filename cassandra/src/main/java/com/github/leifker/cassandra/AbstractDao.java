package com.github.leifker.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.mapping.Mapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by dleifker on 2/14/17.
 * @param <PK> Partition key
 * @param <M> Model
 */
public abstract class AbstractDao<PK, M extends CassandraModel<PK>> {
  private final Session session;

  public AbstractDao(Session session) {
    this.session = Preconditions.checkNotNull(session);
    createIfNotExists();
  }

  /**
   * Persist model
   * @param model to persist
   */
  public void persist(M model) throws Exception {
    persist(ImmutableList.of(model));
  }

  /**
   * Persist collection
   * @param models to persist
   */
  public void persist(Collection<M> models) throws Exception {
    List<ResultSetFuture> futures = new ArrayList<>();
    models.stream()
        .collect(Collectors.groupingBy(CassandraModel::getPartitionKey, Collectors.mapping(Function.identity(), Collectors.toList())))
        .entrySet().forEach(entry -> {
              BatchStatement batch = new BatchStatement();
              entry.getValue().forEach(model -> batch.add(getMapper().saveQuery(model)));
              futures.add(session.executeAsync(batch));
            }
        );
    executeFutures(futures, null);
  }

  public void deleteByKey(PK key) throws Exception {
    deleteByKey(ImmutableList.of(key));
  }

  public void deleteByKey(Collection<PK> keys) throws Exception {
    List<ResultSetFuture> futures = new ArrayList<>();
    keys.forEach(key -> futures.add(session.executeAsync(deleteByPartitionKey(key).setIdempotent(true))));
    executeFutures(futures, null);
  }

  public Stream<M> streamByKey(PK key) throws Exception {
    return streamByKey(ImmutableList.of(key));
  }

  public Stream<M> streamByKey(Collection<PK> keys) throws Exception {
    List<ResultSetFuture> futures = new ArrayList<>();
    keys.forEach(key -> futures.add(session.executeAsync(findByPartitionkey(key).setIdempotent(true))));
    return streamResults(executeFutures(futures, null));
  }

  public void createIfNotExists() {
    session.execute(getDefaultSchema());
  }

  public void truncate() {
    String keyspace = session.getLoggedKeyspace();
    String columnFamily = getMapper().getTableMetadata().getName();
    Statement truncateColumnFamily = new SimpleStatement(String.format("TRUNCATE %s.%s", keyspace, columnFamily));
    session.execute(truncateColumnFamily);
  }

  public void drop() {
    String keyspace = session.getLoggedKeyspace();
    String columnFamily = getMapper().getTableMetadata().getName();
    Statement truncateColumnFamily = new SimpleStatement(String.format("DROP COLUMNFAMILY IF EXISTS %s.%s", keyspace, columnFamily));
    session.execute(truncateColumnFamily);
  }

  protected Stream<M> streamResults(List<ResultSet> resultSets) {
    return StreamSupport.stream(
        Iterables.concat(resultSets.stream().filter(Objects::nonNull)
            .map(getMapper()::map)
            .collect(Collectors.toList())).spliterator(), false);
  }

  protected static List<ResultSet> executeFutures(List<ResultSetFuture> futureResultSets, Duration timeout) throws Exception {
    ListenableFuture<List<ResultSet>> coalesced = Futures.successfulAsList(futureResultSets);
    return timeout == null ? coalesced.get() : coalesced.get(timeout.getSeconds(), TimeUnit.SECONDS);
  }

  protected Session getSession() {
    return session;
  }

  protected abstract Statement deleteByPartitionKey(PK key);

  protected abstract Statement findByPartitionkey(PK key);

  protected abstract Mapper<M> getMapper();

  protected abstract String getDefaultSchema();
}
