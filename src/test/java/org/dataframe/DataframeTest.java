package org.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.stream.Stream;

public class DataframeTest {

    private static final Logger logger = LogManager.getLogger(DataframeTest.class);

    @Mock
    private EntityManager entityManager;

    @Mock
    private Query query;

    @Test
    public void generator() {
        Random random = new Random();

        int maxSize = 1_000_000;
        Dataframe dataframe = new Dataframe.StreamDataframeBuilder(
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "RPLAN100", "RPLAN10", "RPLAN30"),
                Dataframe.choice(random, "VOICE", "DATA"),
                random.doubles(0, 120.0),
                random.doubles(0, 1200.0)
        ).columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").size(maxSize).build();

        Assert.assertEquals(dataframe.sum("CHARGE"), dataframe.sum(4), 0);
        Assert.assertEquals(dataframe.sum("CHARGE"), dataframe.average(4)*dataframe.getNumberOfRows(), 0);
    }


    @Test
    public void testSelect() {
        Random random = new Random();

        int maxSize = 1_000_000;
        Dataframe dataframe = new Dataframe.StreamDataframeBuilder(
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "RPLAN100", "RPLAN10", "RPLAN30"),
                Dataframe.choice(random, "VOICE", "DATA"),
                random.doubles(0, 120.0),
                random.doubles(0, 1200.0)
        ).columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").size(maxSize).build();


        long total = dataframe.selectByName(row -> row.get("TYPE").equals("VOICE")).count() + dataframe.selectByName(row -> row.get("TYPE").equals("DATA")).count();
        Assert.assertEquals(total, dataframe.count());
        Assert.assertEquals(dataframe.sum("CHARGE"), dataframe.average(4)*dataframe.getNumberOfRows(), 0);
    }


    @Test
    public void groupBy() {
        Random random = new Random();

        int maxSize = 1_000_000;
        Dataframe dataframe = new Dataframe.StreamDataframeBuilder(
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "RPLAN100", "RPLAN10", "RPLAN30"),
                Dataframe.choice(random, "VOICE", "DATA"),
                random.doubles(0, 120.0),
                random.doubles(0, 1200.0)
        ).columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").size(maxSize).build();

        Dataframe aggregate = dataframe.groupBy(false, 1, 2).sum("CHARGE", "DURATION");

        Assert.assertEquals(aggregate.count(), 15);
        Assert.assertEquals(aggregate.sum(2), dataframe.sum(4), 0);
        Assert.assertEquals(aggregate.sum("DURATION"), dataframe.sum("DURATION"), 0);
        Assert.assertEquals(aggregate.sum("CHARGE"), dataframe.sum("CHARGE"), 0);
        Assert.assertNotEquals(aggregate.sum("CHARGE"), dataframe.sum("DURATION"), 0);

        aggregate = dataframe.groupBy(true, 1, 2).aggregate("CHARGE", "DURATION");
    }

    @Test
    public void sql() throws Exception {
        MockitoAnnotations.initMocks(this);

        Random random = new Random();
        ArrayList<Object[]> list = new ArrayList();


        int maxSize = 1_000_000;
        Object[] orgCountry = random.ints(0, 5).limit(maxSize).mapToObj(i -> new String[]{"GRE", "ITA", "UK", "ALB", "EU"}[i]).toArray();
        Object[] destCountry = random.ints(0, 5).limit(maxSize).mapToObj(i -> new String[]{"GRE", "ITA", "UK", "ALB", "EU"}[i]).toArray();
        Object[] rplans = random.ints(0, 3).limit(maxSize).mapToObj(i -> new String[]{"RPLAN100", "RPLAN10", "RPLAN30"}[i]).toArray();
        Object[] types = random.ints(0, 3).limit(maxSize).mapToObj(i -> new String[]{"VOICE", "PIBX", "DATA"}[i]).toArray();
        double[] charges = random.doubles(0, 120.0).limit(maxSize).toArray();
        double[] durations = random.doubles(0, 120.0).limit(maxSize).toArray();
        for (int i=0;i< charges.length;i++) {
            list.add(new Object[]{ orgCountry[i], destCountry[i], rplans[i], types[i], charges[i], durations[i]});
        }

        Mockito.when(entityManager.createNativeQuery(ArgumentMatchers.any())).thenReturn(query);
        Mockito.when(query.getResultList()).thenReturn(list);

        Dataframe dataframe = Dataframe.sql(entityManager, "")
                .types(String.class, String.class, String.class, String.class, Number.class, Number.class)
                .columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").build();

        dataframe.sort(5);

    }



}