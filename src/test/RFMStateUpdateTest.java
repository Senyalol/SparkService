package test;

import org.junit.jupiter.api.*;
import spark.SparkStreamingApp;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RFM State Update Logic Tests")
class RFMStateUpdateTest {

    private SparkStreamingApp.RFMState state;
    private long now;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
        now = System.currentTimeMillis();
    }

    @Nested
    @DisplayName("M_total and F_total accumulation tests")
    class TotalAccumulationTests {

        @Test
        @DisplayName("Should accumulate M_total correctly for deposits")
        void shouldAccumulateMTotalForDeposits() {
            // Deposit 100
            updateMTotal(state, 100.0, "Deposit");
            assertEquals(100.0, state.getMTotal(), 0.001);

            // Deposit 50 more
            updateMTotal(state, 50.0, "Deposit");
            assertEquals(150.0, state.getMTotal(), 0.001);
        }

        @Test
        @DisplayName("Should accumulate M_total correctly for credits")
        void shouldAccumulateMTotalForCredits() {
            // Start with 1000
            updateMTotal(state, 1000.0, "Deposit");
            assertEquals(1000.0, state.getMTotal(), 0.001);

            // Credit 300
            updateMTotal(state, 300.0, "Credit");
            assertEquals(700.0, state.getMTotal(), 0.001);
        }

        @Test
        @DisplayName("Should not allow M_total to go negative")
        void shouldNotAllowNegativeMTotal() {
            // Start with 100
            updateMTotal(state, 100.0, "Deposit");

            // Try to credit 200 (would make -100)
            updateMTotal(state, 200.0, "Credit");

            // M_total should be clamped to 0
            assertEquals(0.0, state.getMTotal(), 0.001);
        }

        @Test
        @DisplayName("Should accumulate F_total correctly")
        void shouldAccumulateFTotals() {
            assertEquals(0, state.getFTotal());

            updateFTotal(state, "Deposit");
            assertEquals(1, state.getFTotal());

            updateFTotal(state, "Credit");
            assertEquals(2, state.getFTotal());

            updateFTotal(state, "Deposit");
            assertEquals(3, state.getFTotal());
        }
    }

    @Nested
    @DisplayName("F_window and M_window (windowed metrics) tests")
    class WindowedMetricsTests {

        @Test
        @DisplayName("Should calculate M_window correctly within time window")
        void shouldCalculateMWindowCorrectly() {
            long now = System.currentTimeMillis();

            // Add transactions within window (last 5 minutes)
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 60000, 100.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 120000, 50.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 180000, 30.0, "Credit"));

            double mWindow = SparkStreamingApp.segmentBalanceFromEntries(state.getEntries());
            assertEquals(120.0, mWindow, 0.001); // 100 + 50 - 30 = 120
        }

        @Test
        @DisplayName("Should exclude transactions outside time window")
        void shouldExcludeOldTransactions() {
            long now = System.currentTimeMillis();

            // Old transaction (10 minutes ago)
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 600000, 1000.0, "Deposit"));
            // Recent transaction
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 60000, 100.0, "Deposit"));

            SparkStreamingApp.purgeWindow(state.getEntries(), now, 300000); // 5 min window

            double mWindow = SparkStreamingApp.segmentBalanceFromEntries(state.getEntries());
            assertEquals(100.0, mWindow, 0.001); // Only recent transaction counts
            assertEquals(1, state.getEntries().size());
        }

        @Test
        @DisplayName("F_window should reflect number of transactions in window")
        void shouldCalculateFWindowCorrectly() {
            long now = System.currentTimeMillis();

            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 60000, 100.0, "Deposit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 120000, 50.0, "Credit"));
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now - 600000, 1000.0, "Deposit")); // Old

            SparkStreamingApp.purgeWindow(state.getEntries(), now, 300000);

            assertEquals(2, state.getEntries().size()); // Only recent 2 transactions
        }
    }

    @Nested
    @DisplayName("Credit rejection tests (negative M prevention)")
    class CreditRejectionTests {

        @Test
        @DisplayName("Should reject credit when would cause negative M_window")
        void shouldRejectCreditCausingNegativeMWindow() {
            // Only 100 in window
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now, 100.0, "Deposit"));
            double currentMWindow = SparkStreamingApp.segmentBalanceFromEntries(state.getEntries());

            // Try to credit 150
            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentMWindow, "Credit", 150.0);

            assertTrue(isNegative, "Credit should be rejected");
        }

        @Test
        @DisplayName("Should accept credit when keeps M_window >= 0")
        void shouldAcceptCreditKeepingMWindowNonNegative() {
            state.getEntries().add(new SparkStreamingApp.TransactionEntry(now, 100.0, "Deposit"));
            double currentMWindow = SparkStreamingApp.segmentBalanceFromEntries(state.getEntries());

            // Credit 100 (exact amount)
            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentMWindow, "Credit", 100.0);

            assertFalse(isNegative, "Credit equal to balance should be accepted");
        }
    }

    @Nested
    @DisplayName("First transaction handling tests")
    class FirstTransactionTests {

        @Test
        @DisplayName("Should set firstTs on first transaction")
        void shouldSetFirstTs() {
            long firstTimestamp = now - 60000;

            if (state.getFirstTs() == 0) {
                state.setFirstTs(firstTimestamp);
            }

            assertEquals(firstTimestamp, state.getFirstTs());
        }

        @Test
        @DisplayName("Should not overwrite firstTs on subsequent transactions")
        void shouldNotOverwriteFirstTs() {
            long firstTimestamp = now - 60000;
            state.setFirstTs(firstTimestamp);

            long secondTimestamp = now;
            // Should not update firstTs
            if (state.getFirstTs() == 0) {
                state.setFirstTs(secondTimestamp);
            }

            assertEquals(firstTimestamp, state.getFirstTs(), "FirstTs should not change");
        }
    }

    private void updateMTotal(SparkStreamingApp.RFMState state, double sum, String type) {
        double currentMTotal = state.getMTotal();
        if ("Deposit".equalsIgnoreCase(type)) {
            currentMTotal += sum;
        } else if ("Credit".equalsIgnoreCase(type)) {
            currentMTotal -= sum;
        }
        state.setMTotal(Math.max(0, currentMTotal));
    }

    private void updateFTotal(SparkStreamingApp.RFMState state, String type) {
        state.setFTotal(state.getFTotal() + 1);
    }
}

@DisplayName("RFM Complete Workflow Tests")
class RFMCompleteWorkflowTest {

    private SparkStreamingApp.RFMState state;
    private long baseTime;

    @BeforeEach
    void setUp() {
        state = new SparkStreamingApp.RFMState();
        baseTime = System.currentTimeMillis();
        state.setFirstTs(baseTime - 3600000); // 1 hour ago
    }

    @Test
    @DisplayName("VIP user: high value + many transactions")
    void testVIPUserWorkflow() {
        // VIP conditions: M_total > 10000, F_total > 5
        state.setMTotal(15000.0);
        state.setFTotal(10L);
        state.setRMinutes(2.0);

        String segment = getSegment(state);
        assertEquals("VIP", segment);
    }

    @Test
    @DisplayName("Active user: medium value + some transactions")
    void testActiveUserWorkflow() {
        // Active conditions: M_total > 1000, F_total > 1
        state.setMTotal(5000.0);
        state.setFTotal(3L);
        state.setRMinutes(2.0);

        String segment = getSegment(state);
        assertEquals("Активный", segment);
    }

    @Test
    @DisplayName("Newcomer: registered less than 5 minutes ago")
    void testNewcomerWorkflow() {
        state.setFirstTs(baseTime - 120000); // 2 minutes ago
        state.setMTotal(5000.0);
        state.setFTotal(3L);

        String segment = getSegment(state);
        assertEquals("Новичок", segment);
    }

    @Test
    @DisplayName("Sleeping user: inactive for > 30 minutes")
    void testSleepingUserWorkflow() {
        state.setMTotal(500.0);
        state.setFTotal(1L);
        state.setRMinutes(40.0); // > 30 minutes

        String segment = getSegment(state);
        assertEquals("Спящий", segment);
    }

    @Test
    @DisplayName("Standard user: default fallback")
    void testStandardUserWorkflow() {
        state.setMTotal(500.0);
        state.setFTotal(1L);
        state.setRMinutes(10.0); // < 30 minutes

        String segment = getSegment(state);
        assertEquals("Стандартный", segment);
    }

    @Test
    @DisplayName("State evolves correctly over time")
    void testStateEvolution() {
        long now = System.currentTimeMillis();

        // Transaction 1: Deposit 1000 (new user)
        updateState(state, now - 60000, 1000.0, "Deposit");
        assertTrue(state.getFirstTs() > 0);
        assertEquals(1000.0, state.getMTotal(), 0.001);
        assertEquals(1, state.getFTotal());

        // Transaction 2: Credit 200 (after 1 minute)
        updateState(state, now, 200.0, "Credit");
        assertEquals(800.0, state.getMTotal(), 0.001);
        assertEquals(2, state.getFTotal());

        // Transaction 3: Deposit 5000 (makes VIP eligible)
        updateState(state, now + 60000, 5000.0, "Deposit");
        assertEquals(5800.0, state.getMTotal(), 0.001);
        assertEquals(3, state.getFTotal());
    }

    private void updateState(SparkStreamingApp.RFMState state, long timestamp, double sum, String type) {
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
    }

    private String getSegment(SparkStreamingApp.RFMState state) {
        double firstHoursAgo = (System.currentTimeMillis() - state.getFirstTs()) / 3600000.0;

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
}