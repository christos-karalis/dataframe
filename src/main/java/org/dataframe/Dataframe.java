package org.dataframe;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Dataframe {

    private final String[] columnNames;
    private final Object[][] data;
    private final int numberOfRows;

    private Dataframe(Object[][] data, int numberOfRows) {
        this.data = data;
        this.columnNames = null;
        this.numberOfRows = numberOfRows;
    }

    private Dataframe(Object[][] data,
                      String[] columnNames,
                      int numberOfRows) {
        this.data = data;
        this.columnNames = columnNames;
        this.numberOfRows = numberOfRows;
    }

    public static Stream<String> choice(Random random, String... choices) {
        return random.ints(0, choices.length).mapToObj(i->choices[i]);
    }

    public static SqlDataframeBuilder sql(EntityManager entityManager, String select_) {
        return new SqlDataframeBuilder(entityManager, select_);
    }

    public Dataframe sort(int index) {
        BTreeIndex root = new BTreeIndex(0, index);
        for (int i = 1; i<data[index].length; i++) {
            new BTreeIndex(i, index).insert(root);
        }
        Object[][] data = new Object[Dataframe.this.data.length][];
        for (int i = 0; i< Dataframe.this.data.length ; i++) {
            data[i] = ofType(Dataframe.this.data[i][0].getClass(), Dataframe.this.data[i].length);
        }

        root.sort(false, data);
        return new Dataframe(data, columnNames, numberOfRows);
    }

    private static <T> T[] ofType(Class<T> type, int length) {
        return (T[]) Array.newInstance(type, length);
    }

    public static class StreamDataframeBuilder {
        private BaseStream[] stream;
        private String[] columnNames;
        private int numberOfRows = 100;

        public StreamDataframeBuilder(BaseStream... stream) {
            this.stream = stream;
        }

        public StreamDataframeBuilder columnNames(String... columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public StreamDataframeBuilder size(int numberOfRows) {
            this.numberOfRows = numberOfRows;
            return this;
        }

        public Dataframe build() {
            if ((columnNames!=null && columnNames.length>0)&&columnNames.length!=stream.length) {
                throw new IllegalArgumentException();
            }
            Object[][] data = new Object[stream.length][];
            AtomicInteger streamIndex = new AtomicInteger();
            for (BaseStream columnStream : stream) {
                int columnIndex = streamIndex.getAndIncrement();
                Iterator iterator = columnStream.iterator();
                ArrayList temp = new ArrayList<>();
                int count = 0;
                while (iterator.hasNext() && count<numberOfRows) {
                    Object cell = iterator.next();
                    temp.add(cell);
                    count++;
                }
                numberOfRows = temp.size();
                data[columnIndex] = temp.toArray(ofType(temp.get(0).getClass(), numberOfRows));
            }

            if ((columnNames!=null && columnNames.length>0)) {
                return new Dataframe(data, columnNames, numberOfRows);
            } else {
                return new Dataframe(data, numberOfRows);
            }
        }
    }

    public static class SqlDataframeBuilder {
        private Query query;
        private String[] columnNames;
        private Class[] columnTypes;
        int parameterCount = 1;
        Map<Integer, Class> types = new TreeMap<>();
        Map<String, Class> typesByName = new TreeMap<>();


        public SqlDataframeBuilder(EntityManager entityManager, String sql) {
            query = entityManager.createNativeQuery(sql);
        }

        public SqlDataframeBuilder addParameter(Object parameter) {
            query.setParameter(parameterCount++, parameter);
            return this;
        }

        public SqlDataframeBuilder setParameter(String parameterName, Object parameter) {
            query.setParameter(parameterName, parameter);
            return this;
        }

        public SqlDataframeBuilder columnNames(String... columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public SqlDataframeBuilder types(Class... columnTypes) {
            this.columnTypes = columnTypes;
            return this;
        }

        public SqlDataframeBuilder setType(String parameterName, Class parameterType) {
            typesByName.put(parameterName, parameterType);
            return this;
        }

        public SqlDataframeBuilder setType(Integer parameterOrder, Class parameterType) {
            types.put(parameterOrder, parameterType);
            return this;
        }

        public Dataframe build() {
            Object[][] data = null;

            List<Object[]> resultList = query.getResultList();
            int size = resultList.size();
            if (columnTypes!=null) {
                data = new Object[columnTypes.length][];
                for (int i = 0; i< columnTypes.length; i++) {
                    data[i] = ofType(columnTypes[i], size);
                }
            }
            for (int rowCount = 0; rowCount < size; rowCount++) {
                Object[] row = resultList.get(rowCount);
                //TODO problematic on cases where first row contains null even if rest data is ok
                if (data == null) {
                    data = new Object[row.length][];
                    for (int columnCount = 0; columnCount < row.length; columnCount++) {
                        Object cell = row[columnCount];
                        if (cell instanceof String) {
                            data[columnCount] = new String[size];
                        } else if (cell instanceof BigDecimal) {
                            data[columnCount] = new Number[size];
                        }
                    }
                }
                for (int columnCount = 0; columnCount < row.length; columnCount++) {
                    Object cell = row[columnCount];
                    data[columnCount][rowCount] = cell;
                }

            }
            if (columnNames==null) {
                return new Dataframe(data, size);
            } else {
                return new Dataframe(data, columnNames, size);
            }
        }
    }



    public class DataframeGroupBy {
        private Map<CompositeKey, List<Integer>> map;
        private int keySize;

        private DataframeGroupBy(Map<CompositeKey, List<Integer>> map, int keySize) {
            this.map = map;
            this.keySize = keySize;
        }

        public Dataframe aggregate(String... columnName) {
            int[] indexes = Stream.of(columnName).mapToInt(Dataframe.this::findColumnIndexByName).toArray();
            return aggregate(indexes);
        }

        /**
         * Creates a new dataframe that contains grouped by indexed columns once per
         * @param index
         * @return
         */
        public Dataframe aggregate(int... index) {
            Object[][] data = new Object[map.keySet().iterator().next().keys.length+index.length][];
            AtomicInteger counter = new AtomicInteger(0);
            map.forEach((compositeKey, value) -> {
                int rowIndex = counter.getAndIncrement();
                initialize(data, rowIndex, compositeKey);
                IntStream.range(0, index.length).forEach(indexIndex -> {
                    if (data[keySize+indexIndex]==null) {
                        data[keySize+indexIndex] = new DoubleSummaryStatistics[map.size()];
                    }
                    data[keySize+indexIndex][rowIndex] = value.stream().map(val -> Dataframe.this.data[index[indexIndex]][val])
                            .filter(Number.class::isInstance).map(Number.class::cast)
                            .collect(Collectors.summarizingDouble(Number::doubleValue));
                });
            });
            return new Dataframe(data, map.values().size());
        }

        private void initialize(Object[][] data, int counter, CompositeKey compositeKey) {
            IntStream.range(0, keySize)
                    .forEach(i -> {
                        if (data[i]==null) {
                            data[i] = new String[map.size()];
                        }
                        data[i][counter] = compositeKey.keys[i];
                    });
        }
    }

    public class BTreeIndex {
        private BTreeIndex asc;
        private BTreeIndex desc;
        private int columnIndex;
        private int index;

        private BTreeIndex(int index, int columnIndex) {
            this.index = index;
            this.columnIndex = columnIndex;
        }

        public BTreeIndex asc() {
            return asc;
        }

        public BTreeIndex desc() {
            return desc;
        }

        public BTreeIndex insert(BTreeIndex root) {
            if (((Number) data[columnIndex][index]).doubleValue() >
                    ((Number) data[root.columnIndex][root.index]).doubleValue()) {
                if (root.asc==null) {
                    root.asc = this;
                } else {
                    insert(root.asc);
                }
            } else {
                if (root.desc==null) {
                    root.desc = this;
                } else {
                    insert(root.desc);
                }
            }
            return asc;
        }

        public void sort(boolean ascending, Object[][] data) {
            sort(ascending, data, new AtomicInteger());
        }


        public void sort(boolean ascending, Object[][] data, AtomicInteger counter) {
            if (!ascending && this.asc != null) {
                this.asc.sort(ascending, data, counter);
            } else if (ascending && this.desc != null) {
                this.desc.sort(ascending, data, counter);
            }
            int counterIndex = counter.getAndIncrement();
            for (int i = 0; i<data.length;i++) {
                data[i][counterIndex] = Dataframe.this.data[i][index];
            }
            if (!ascending && this.desc != null) {
                this.desc.sort(ascending, data, counter);
            } else if (ascending && this.asc != null) {
                this.asc.sort(ascending, data, counter);
            }
        }

    }

    public static class IndexEntry<T> implements Map.Entry<Integer, T> {
        private Integer key;
        private T value;

        public IndexEntry(Integer key, T value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Integer getKey() {
            return key;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public T setValue(T value) {
            return null;
        }
    }

    private Integer findColumnIndexByName(String columnName) {
        Optional<IndexEntry> match = IntStream.range(0, this.columnNames.length)
                .mapToObj(ind -> new IndexEntry(ind, this.columnNames[ind]))
                .filter(indexEntry -> indexEntry.getValue().equals(columnName)).findFirst();
        return match.isPresent() ? match.get().getKey() : -1;//will break subsequent method
    }

    public DataframeGroupBy groupBy(String... columnNames) {
        Integer[] indices = Stream.of(columnNames).map(this::findColumnIndexByName)
                .toArray(Integer[]::new);
        return groupBy(indices);
    }

    /**
     *
     * @param indices column indices to group by
     * @return
     */
    public DataframeGroupBy groupBy(Integer... indices) {
        Map<CompositeKey, List<Integer>> collect = IntStream.range(0, numberOfRows)
                .mapToObj(rowC -> {
                    Object[] keys = IntStream.range(0, indices.length)
                            .mapToObj(i -> data[indices[i]][rowC]).toArray(Object[]::new);
                    return new IndexEntry<CompositeKey>(rowC, new CompositeKey(keys));
                }).collect(Collectors.groupingBy(IndexEntry::getValue, Collectors.mapping(IndexEntry::getKey, Collectors.toList())));
        return new DataframeGroupBy(collect, indices.length);
    }

    public double sum(int index) {
        Stream<Object> data = Stream.of(this.data[index]);
        DoubleSummaryStatistics collect = data.collect(Collectors.summarizingDouble(row -> ((Number) row).doubleValue()));
        return collect.getSum();
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        Stream.of(data)
                .forEach(row -> s.append(Arrays.toString(row)));

        return "Dataframe{" +
                "data=" + s.toString() +
                '}';
    }

    private static class CompositeKey {
        private Object[] keys;

        public CompositeKey(Object... keys) {
            this.keys = keys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CompositeKey that = (CompositeKey) o;

            return Arrays.equals(keys, that.keys);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(keys);
        }
    }
}
