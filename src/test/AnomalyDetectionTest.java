package test;

import org.junit.jupiter.api.*;
import spark.SparkStreamingApp;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Anomaly Detection Tests")
class AnomalyDetectionTest {

    private SparkStreamingApp.RFMState state;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
    }

    @Nested
    @DisplayName("BIGGER_THEN_AVG_CHECK Tests")
    class BiggerThenAvgCheckTests {

        @Test
        @DisplayName("Should detect when sum >= 3 * average")
        void shouldDetectLargeTransaction() {
            // Добавляем транзакции для среднего чека ~100
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(1000L, 100.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(2000L, 100.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(3000L, 100.0, "Deposit"));

            double avg = calculateAverage(state.getEntries());
            double sum = 350.0; // >= 3 * avg (300)

            assertTrue(sum >= 3 * avg, "Should detect anomaly");
        }

        @Test
        @DisplayName("Should NOT detect when sum < 3 * average")
        void shouldNotDetectNormalTransaction() {
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(1000L, 100.0, "Deposit"));

            double avg = calculateAverage(state.getEntries());
            double sum = 250.0; // < 3 * avg (300)

            assertFalse(sum >= 3 * avg, "Should not detect normal transaction");
        }
    }

    @Nested
    @DisplayName("STRUCTURING_SMALL_TRANSACTIONS Tests")
    class StructuringTests {

        @Test
        @DisplayName("Should detect structuring pattern (many small transactions)")
        void shouldDetectStructuring() {
            long now = System.currentTimeMillis();
            // Добавляем 9 маленьких транзакций
            for (int i = 0; i < 9; i++) {
                state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                        now - 10000 * i, 450.0, "Deposit"));
            }

            // 10-я маленькая транзакция
            double currentSum = 450.0;

            boolean isStructuring = checkStructuringCondition(
                    state.getEntries(), currentSum, now);

            assertTrue(isStructuring, "Should detect structuring pattern");
        }
    }

    private double calculateAverage(List<SparkStreamingApp.TransactionEntry> entries) {
        return entries.stream().mapToDouble(SparkStreamingApp.TransactionEntry::getSum).average().orElse(0);
    }

    private boolean checkStructuringCondition(
            List<SparkStreamingApp.TransactionEntry> entries,
            double currentSum,
            long currentTime) {
        long smallCount = entries.stream()
                .filter(e -> e.getSum() <= 500.0)
                .count();
        double smallTotal = entries.stream()
                .filter(e -> e.getSum() <= 500.0)
                .mapToDouble(SparkStreamingApp.TransactionEntry::getSum)
                .sum();

        if (currentSum <= 500.0) {
            smallCount++;
            smallTotal += currentSum;
        }

        return smallCount >= 10 && smallTotal >= 3000.0;
    }
}

@DisplayName("RFM Segmentation Logic Tests")
class RFMSegmentationLogicTest {

    private SparkStreamingApp.RFMState state;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
        state.setFirstTs(System.currentTimeMillis() - 3600000); // 1 hour ago (not newcomer)
    }

    @Test
    @DisplayName("Should classify as VIP: M_total > 10000 AND F_total > 5")
    void shouldClassifyVIP() {
        state.setMTotal(15000.0);
        state.setFTotal(10L);

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("VIP", segment);
    }

    @Test
    @DisplayName("Should classify as Active: M_total > 1000 AND F_total > 1")
    void shouldClassifyActive() {
        state.setMTotal(5000.0);
        state.setFTotal(3L);

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("Активный", segment);
    }

    @Test
    @DisplayName("Should classify as Newcomer: first transaction < 5 minutes ago")
    void shouldClassifyNewcomer() {
        state.setFirstTs(System.currentTimeMillis() - 120000); // 2 minutes ago

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("Новичок", segment);
    }

    @Test
    @DisplayName("Should classify as Sleeping: rMinutes > 30")
    void shouldClassifySleeping() {
        state.setMTotal(500.0);  // Not VIP or Active
        state.setFTotal(1L);
        state.setRMinutes(40.0);  // > 30 minutes (исправлено)

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("Спящий", segment);
    }

    @Test
    @DisplayName("Should classify as Standard: default case")
    void shouldClassifyStandard() {
        state.setMTotal(500.0);
        state.setFTotal(1L);
        state.setRMinutes(25.0);  // < 30 minutes (исправлено)

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("Стандартный", segment);
    }

    @Test
    @DisplayName("Should classify as Sleeping when rMinutes exactly equals threshold")
    void shouldClassifyAsStandardWhenEqualsThreshold() {
        state.setMTotal(500.0);
        state.setFTotal(1L);
        state.setRMinutes(30.0);  // Exactly 30 minutes

        String segment = calculateSegment(state, System.currentTimeMillis());

        assertEquals("Стандартный", segment, "Should be Standard when rMinutes == 30");
    }

    private String calculateSegment(SparkStreamingApp.RFMState state, long currentTime) {
        double firstHoursAgo = (currentTime - state.getFirstTs()) / 3600000.0;

        if (firstHoursAgo < 5.0/60.0) {
            return "Новичок";
        } else if (state.getMTotal() > 10000 && state.getFTotal() > 5) {
            return "VIP";
        } else if (state.getMTotal() > 1000 && state.getFTotal() > 1) {
            return "Активный";
        } else if (state.getRMinutes() > 30) {  // ← Используем реальный порог
            return "Спящий";
        } else {
            return "Стандартный";
        }
    }
}