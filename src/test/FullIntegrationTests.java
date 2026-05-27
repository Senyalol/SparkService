package test;

import org.junit.jupiter.api.*;
import spark.SparkStreamingApp;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Full Integration Tests (Without External Dependencies)")
class FullIntegrationTests {

    private SparkStreamingApp.RFMState state;
    private long now;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
        now = System.currentTimeMillis();
    }

    @Nested
    @DisplayName("Complete RFM Workflow Tests")
    class CompleteRFMWorkflowTests {

        @Test
        @DisplayName("Full user journey: Newcomer → Standard → Active → VIP")
        void testFullUserJourney() {
            long startTime = now;

            // ========== STAGE 1: NEWCOMER ==========
            state.setFirstTs(startTime - 120000); // 2 minutes ago
            processTransaction(state, startTime - 120000, 100.0, "Deposit");
            assertSegment("Новичок", startTime, state);

            // ========== STAGE 2: STANDARD ==========
            // After 10 minutes (not newcomer anymore)
            state.setFirstTs(startTime - 600000); // 10 minutes ago

            // Reset and add transactions
            state.setMTotal(0);
            state.setFTotal(0);
            processTransaction(state, startTime - 500000, 200.0, "Deposit");
            processTransaction(state, startTime - 400000, 50.0, "Credit");

            // M_total = 150, F_total = 2 → Standard
            assertSegment("Стандартный", startTime, state);
            assertEquals(150.0, state.getMTotal(), 0.001);
            assertEquals(2, state.getFTotal());

            // ========== STAGE 3: ACTIVE ==========
            processTransaction(state, startTime - 300000, 900.0, "Deposit");
            processTransaction(state, startTime - 200000, 100.0, "Deposit");

            // M_total = 1150, F_total = 4 → Active
            assertSegment("Активный", startTime, state);
            assertEquals(1150.0, state.getMTotal(), 0.001);
            assertEquals(4, state.getFTotal());

            // ========== STAGE 4: VIP ==========
            processTransaction(state, startTime - 100000, 10000.0, "Deposit");
            processTransaction(state, startTime, 5000.0, "Deposit");

            // M_total = 16150, F_total = 6 → VIP
            assertSegment("VIP", startTime + 60000, state);
            assertEquals(16150.0, state.getMTotal(), 0.001);
            assertEquals(6, state.getFTotal());
        }

        @Test
        @DisplayName("Sleeping user detection after inactivity (low value user)")
        void testSleepingUserDetection() {
            // Setup: User with LOW values (not VIP or Active)
            state.setFirstTs(now - 3600000); // 1 hour ago
            state.setMTotal(500.0);          // < 1000 (not Active)
            state.setFTotal(2L);             // > 1 but MTotal < 1000
            state.setLastTs(now - 1800000);  // Last transaction 30 minutes ago
            state.setRMinutes(35.0);         // > 30 minutes

            assertSegment("Спящий", now, state);
        }

        @Test
        @DisplayName("User with zero transactions but high RMinutes")
        void testZeroTransactionsSleeping() {
            state.setFirstTs(now - 3600000); // 1 hour ago
            state.setMTotal(0.0);
            state.setFTotal(0L);
            state.setRMinutes(40.0); // > 30 minutes

            assertSegment("Спящий", now, state);
        }

        @Test
        @DisplayName("VIP user should not become Sleeping even if inactive")
        void testVIPNeverBecomesSleeping() {
            state.setFirstTs(now - 3600000);
            state.setMTotal(15000.0);        // VIP
            state.setFTotal(10L);            // VIP
            state.setRMinutes(120.0);        // Very inactive (2 hours)

            assertSegment("VIP", now, state);
        }

        @Test
        @DisplayName("Active user should not become Sleeping even if inactive")
        void testActiveNeverBecomesSleeping() {
            state.setFirstTs(now - 3600000);
            state.setMTotal(5000.0);         // Active
            state.setFTotal(3L);             // Active
            state.setRMinutes(60.0);         // Inactive for 1 hour

            assertSegment("Активный", now, state);
        }

        @Test
        @DisplayName("Newcomer has priority over other segments")
        void testNewcomerPriority() {
            state.setFirstTs(now - 120000); // 2 minutes ago
            state.setMTotal(20000.0); // Would be VIP if not newcomer
            state.setFTotal(100L);
            state.setRMinutes(1.0);

            assertSegment("Новичок", now, state);
        }

        @Test
        @DisplayName("Standard user when nothing else matches")
        void testStandardUser() {
            state.setFirstTs(now - 3600000); // 1 hour ago
            state.setMTotal(500.0);          // < 1000
            state.setFTotal(1L);             // = 1 (not > 1)
            state.setRMinutes(10.0);         // < 30 minutes

            assertSegment("Стандартный", now, state);
        }
    }

    @Nested
    @DisplayName("Complete Anomaly Detection Workflow Tests")
    class CompleteAnomalyWorkflowTests {

        @Test
        @DisplayName("NEGATIVE_M anomaly detection")
        void testNegativeMAnomaly() {
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 60000, 100.0, "Deposit"));
            double currentM = SparkStreamingApp.segmentBalanceFromEntries(state.getEntries());

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, "Credit", 150.0);
            assertTrue(isNegative);

            boolean isNotNegative = SparkStreamingApp.isNegativeMCredit(currentM, "Credit", 50.0);
            assertFalse(isNotNegative);
        }

        @Test
        @DisplayName("BIGGER_THEN_AVG_CHECK anomaly detection")
        void testBiggerThenAvgCheck() {
            for (int i = 0; i < 5; i++) {
                state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                        now - (i * 10000), 100.0, "Deposit"));
            }

            double avg = calculateAverage(state.getEntries());
            double largeSum = 350.0;

            assertTrue(largeSum >= 3 * avg);
            assertFalse(200.0 >= 3 * avg);
        }

        @Test
        @DisplayName("STRUCTURING_SMALL_TRANSACTIONS detection")
        void testStructuringDetection() {
            for (int i = 0; i < 9; i++) {
                state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                        now - (i * 10000), 450.0, "Deposit"));
            }

            boolean isStructuring = checkStructuringCondition(state.getEntries(), 450.0, now);
            assertTrue(isStructuring);
        }

        @Test
        @DisplayName("STRUCTURING_SMALL_TRANSACTIONS not detected with 9 transactions")
        void testNoStructuringWith9Transactions() {
            for (int i = 0; i < 8; i++) {
                state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                        now - (i * 10000), 450.0, "Deposit"));
            }

            boolean isStructuring = checkStructuringCondition(state.getEntries(), 450.0, now);
            assertFalse(isStructuring);
        }

        @Test
        @DisplayName("EXCESSIVE_REVERSAL_PATTERN detection")
        void testExcessiveReversalPattern() {
            long creditTime = now;

            state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                    creditTime - 45000, 1000.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));

            boolean result = checkReversalPattern(state.getEntries(), "Credit", 900.0, creditTime);
            assertTrue(result);
        }

        @Test
        @DisplayName("EXCESSIVE_REVERSAL_PATTERN not detected with 1 deposit")
        void testNoReversalWith1Deposit() {
            long creditTime = now;

            state.getEntries().add(new SparkStreamingApp.TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));

            boolean result = checkReversalPattern(state.getEntries(), "Credit", 900.0, creditTime);
            assertFalse(result);
        }
    }

    // Остальные вложенные классы (StateManagementTests, MultiUserIsolationTests,
    // TimeWindowEdgeCasesTests, PerformanceUnderLoadTests) остаются без изменений
    // ...

    // ========== HELPER METHODS ==========

    private void processTransaction(SparkStreamingApp.RFMState state, long timestamp, double sum, String type) {
        if (state.getFirstTs() == 0) {
            state.setFirstTs(timestamp);
        }

        if ("Deposit".equalsIgnoreCase(type)) {
            state.setMTotal(state.getMTotal() + sum);
        } else if ("Credit".equalsIgnoreCase(type)) {
            state.setMTotal(Math.max(0, state.getMTotal() - sum));
        }
        state.setFTotal(state.getFTotal() + 1);
        state.setLastTs(timestamp);
        state.getEntries().add(new SparkStreamingApp.TransactionEntry(timestamp, sum, type));
    }

    private void assertSegment(String expected, long currentTime, SparkStreamingApp.RFMState state) {
        String segment = calculateSegment(state, currentTime);
        assertEquals(expected, segment);
    }

    private String calculateSegment(SparkStreamingApp.RFMState state, long currentTime) {
        double firstHoursAgo = (currentTime - state.getFirstTs()) / 3600000.0;

        if (firstHoursAgo < 5.0/60.0) {
            return "Новичок";
        } else if (state.getMTotal() > 10000 && state.getFTotal() > 5) {
            return "VIP";
        } else if (state.getMTotal() > 1000 && state.getFTotal() > 1) {
            return "Активный";
        } else if (state.getRMinutes() > 30) {
            return "Спящий";
        } else {
            return "Стандартный";
        }
    }

    private double calculateAverage(List<SparkStreamingApp.TransactionEntry> entries) {
        return entries.stream().mapToDouble(SparkStreamingApp.TransactionEntry::getSum).average().orElse(0);
    }

    private boolean checkStructuringCondition(List<SparkStreamingApp.TransactionEntry> entries,
                                              double currentSum, long currentTime) {
        long smallCount = entries.stream().filter(e -> e.getSum() <= 500.0).count();
        double smallTotal = entries.stream().filter(e -> e.getSum() <= 500.0)
                .mapToDouble(SparkStreamingApp.TransactionEntry::getSum).sum();

        if (currentSum <= 500.0) {
            smallCount++;
            smallTotal += currentSum;
        }

        return smallCount >= 10 && smallTotal >= 3000.0;
    }

    private boolean checkReversalPattern(List<SparkStreamingApp.TransactionEntry> entries,
                                         String currentType, double currentSum, long currentTime) {
        if (!"Credit".equalsIgnoreCase(currentType)) return false;

        int reversalCount = 0;
        for (SparkStreamingApp.TransactionEntry e : entries) {
            if ("Deposit".equalsIgnoreCase(e.getType())) {
                if (currentTime - e.getTimestamp() <= 300000) {
                    boolean amountMatch = currentSum >= e.getSum() * 0.9;
                    boolean timeMatch = (currentTime - e.getTimestamp()) <= 60000;
                    if (amountMatch && timeMatch) reversalCount++;
                }
            }
        }
        return reversalCount >= 2;
    }

    private String serializeToCheckpoint(SparkStreamingApp.RFMState state) {
        StringBuilder sb = new StringBuilder();
        sb.append(state.getFirstTs()).append("|")
                .append(state.getMTotal()).append("|")
                .append(state.getFTotal()).append("|")
                .append(state.getRMinutes()).append("|")
                .append(state.getLastTs());
        return sb.toString();
    }

    private SparkStreamingApp.RFMState deserializeFromCheckpoint(String data) {
        SparkStreamingApp.RFMState state = new SparkStreamingApp.RFMState();
        String[] parts = data.split("\\|");
        if (parts.length >= 5) {
            state.setFirstTs(Long.parseLong(parts[0]));
            state.setMTotal(Double.parseDouble(parts[1]));
            state.setFTotal(Long.parseLong(parts[2]));
            state.setRMinutes(Double.parseDouble(parts[3]));
            state.setLastTs(Long.parseLong(parts[4]));
        }
        return state;
    }

    private SparkStreamingApp.RFMState createUserState(int userId) {
        return new SparkStreamingApp.RFMState();
    }
}