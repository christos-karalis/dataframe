package org.dataframe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import java.util.ArrayList;
import java.util.Random;

public class DataframeTest {

    private static final Logger logger = LogManager.getLogger(DataframeTest.class);

    @Mock
    private EntityManager entityManager;

    @Mock
    private Query query;

    @Test
    public void sql() throws Exception {
        MockitoAnnotations.initMocks(this);

        Random random = new Random();
        ArrayList<Object[]> list = new ArrayList();


        int maxSize = 100000;
        Object[] orgCountry = random.ints(0, 5).limit(maxSize).mapToObj(i -> new String[]{"GRE", "ITA", "UK", "ALB", "EU"}[i]).toArray();
        Object[] destCountry = random.ints(0, 5).limit(maxSize).mapToObj(i -> new String[]{"GRE", "ITA", "UK", "ALB", "EU"}[i]).toArray();
        Object[] rplans = random.ints(0, 3).limit(maxSize).mapToObj(i -> new String[]{"RPLAN100", "RPLAN10", "RPLAN30"}[i]).toArray();
        Object[] types = random.ints(0, 3).limit(maxSize).mapToObj(i -> new String[]{"VOICE", "PIBX", "DATA"}[i]).toArray();
        double[] charges = random.doubles(0, 120.0).limit(maxSize).toArray();
        double[] durations = random.doubles(0, 120.0).limit(maxSize).toArray();
        for (int i=0;i< charges.length;i++) {
            list.add(new Object[]{ orgCountry[i], destCountry[i], rplans[i], types[i], charges[i], durations[i]});
        }
        Dataframe dataframe = new Dataframe.StreamDataframeBuilder(
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "GRE", "ITA", "UK", "ALB", "EU"),
                Dataframe.choice(random, "RPLAN100", "RPLAN10", "RPLAN30"),
                Dataframe.choice(random, "VOICE", "PIBX", "DATA"),
                random.doubles(0, 120.0),
                random.doubles(0, 1200.0)
        ).columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").size(10000).build();
//        logger.debug(
//            dataframe.groupBy(2).aggregate(4, 5)
//        );
        Dataframe aggregate = dataframe.groupBy(1, 2).aggregate("CHARGE", "DURATION");
//        logger.debug(
//                aggregate
//        );
//        list.add(new Object[]{ "RPLAN100", BigDecimal.valueOf(10.1), "VOICE"});
//        list.add(new Object[]{ "RPLAN100", BigDecimal.valueOf(10.1), "DATA"});
//        list.add(new Object[]{ "RPLAN10", BigDecimal.valueOf(10.1), "DATA"});
//        list.add(new Object[]{ null, BigDecimal.valueOf(10.1), "DATA"});

        Mockito.when(entityManager.createNativeQuery(ArgumentMatchers.any())).thenReturn(query);
        Mockito.when(query.getResultList()).thenReturn(list);

        dataframe = Dataframe.sql(entityManager, "")
                .types(String.class, String.class, String.class, String.class, Number.class, Number.class)
                .columnNames("ORG_COUNTRY", "DEST_COUNTRY", "RATE_PLAN", "TYPE", "CHARGE", "DURATION").build();

        dataframe.sort(5);

//        dataframe.groupBy(0).values().stream().forEach(value -> {logger.debug(value.size());});
//        dataframe.groupBy(0).sum(1).sum(1);
//        logger.debug(dataframe.groupBy(0).aggregate(new int[]{4}));
//        logger.debug(dataframe.groupBy(2).aggregate(4));
//        logger.debug(dataframe.groupBy(2, 0).aggregate(4));
//        logger.debug(dataframe.groupBy("RATE_PLAN").aggregate(4));
//        logger.debug(dataframe.groupBy(new Integer[]{2, 0}).aggregate(4));

//        logger.debug(dataframe.groupBy("TYPE").aggregate(4));
//        logger.debug(dataframe);

        logger.debug(dataframe.sum(4));
    }

}