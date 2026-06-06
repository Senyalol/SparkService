package test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import spark.TransactionEntry;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Structuring Small Transactions Tests")
class StructuringSmallTransactionsTest {

    private List<TransactionEntry> entries;
    private long now;

    @BeforeEach
    void setUp() {
        entries = new ArrayList<>();
        now = System.currentTimeMillis();
    }

    @Nested
    @DisplayName("Detection logic tests")
    class DetectionTests {

        @Test
        @DisplayName("Should detect structuring: 10+ small transactions >= 3000 total")
        void shouldDetectStructuringWith10Transactions() {
            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 450.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertTrue(result, "Should detect structuring pattern with 10 transactions");
        }

        @Test
        @DisplayName("Should detect structuring with exactly 10 transactions")
        void shouldDetectWithExactly10Transactions() {
            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 300.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 300.0, now);

            assertTrue(result, "Should detect structuring with exactly 10 transactions and 3000 total");
        }

        @Test
        @DisplayName("Should NOT detect with only 9 small transactions")
        void shouldNotDetectWith9Transactions() {
            for (int i = 0; i < 8; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 450.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertFalse(result, "Should NOT detect structuring with only 9 transactions");
        }

        @Test
        @DisplayName("Should NOT detect when total amount < 3000")
        void shouldNotDetectWhenTotalLessThan3000() {
            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 290.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 290.0, now);

            assertFalse(result, "Should NOT detect when total < 3000");
        }

        @Test
        @DisplayName("Should NOT detect when transactions are large (> 500)")
        void shouldNotDetectLargeTransactions() {
            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 600.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 600.0, now);

            assertFalse(result, "Should NOT detect when transactions > 500");
        }
    }

    @Nested
    @DisplayName("Time window tests")
    class TimeWindowTests {

        @Test
        @DisplayName("Should only count transactions within 10 minute window")
        void shouldOnlyCountTransactionsWithinWindow() {
            // Старая транзакция (15 минут назад) - НЕ должна учитываться
            entries.add(new TransactionEntry(
                    now - 900000, 450.0, "Deposit"));

            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 30000), 450.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertTrue(result, "Should detect structuring ignoring old transactions");
        }

        @Test
        @DisplayName("Should include transactions exactly at window boundary (<=)")
        void shouldIncludeBoundaryTransactions() {
            // Транзакция ровно на границе окна (10 минут) - ДОЛЖНА учитываться
            entries.add(new TransactionEntry(
                    now - 600000, 450.0, "Deposit"));

            for (int i = 0; i < 8; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 30000), 450.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertTrue(result, "Should include transactions exactly at window boundary");
        }

        @Test
        @DisplayName("Should exclude transactions just outside window (> 10 minutes)")
        void shouldExcludeTransactionsJustOutsideWindow() {
            // Транзакция чуть за границей окна (10 минут 1 секунда)
            entries.add(new TransactionEntry(
                    now - 600001, 450.0, "Deposit"));

            for (int i = 0; i < 8; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 30000), 450.0, "Deposit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertFalse(result, "Should NOT include transactions just outside window");
        }
    }

    @Nested
    @DisplayName("Mixed transaction types tests")
    class MixedTypesTests {

        @Test
        @DisplayName("Should count both Deposit and Credit as transactions")
        void shouldCountBothTypes() {
            for (int i = 0; i < 5; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 300.0, "Deposit"));
            }
            for (int i = 0; i < 4; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 300.0, "Credit"));
            }

            boolean result = isStructuringSmallTransactions(entries, 300.0, now);

            assertTrue(result, "Should count both Deposit and Credit transactions");
        }

        @Test
        @DisplayName("Should count transactions even with unknown types")
        void shouldCountUnknownTypes() {
            for (int i = 0; i < 9; i++) {
                entries.add(new TransactionEntry(
                        now - (i * 10000), 450.0, "Unknown"));
            }

            boolean result = isStructuringSmallTransactions(entries, 450.0, now);

            assertTrue(result, "Should count transactions regardless of type");
        }
    }

    // Вспомогательный метод, копирующий логику из SparkStreamingApp
    private boolean isStructuringSmallTransactions(
            List<TransactionEntry> entries,
            double currentSum,
            long currentTime) {

        long smallCount = 0;
        double smallTotal = 0;
        long STRUCTURING_WINDOW_MS = 600000; // 10 minutes
        double SMALL_TRANSACTION_THRESHOLD = 500.0;
        int MIN_STRUCTURING_COUNT = 10;
        double MIN_STRUCTURING_TOTAL = 3000.0;

        for (TransactionEntry e : entries) {
            if (currentTime - e.getTimestamp() <= STRUCTURING_WINDOW_MS) {
                if (e.getSum() <= SMALL_TRANSACTION_THRESHOLD) {
                    smallCount++;
                    smallTotal += e.getSum();
                }
            }
        }

        if (currentSum <= SMALL_TRANSACTION_THRESHOLD) {
            smallCount++;
            smallTotal += currentSum;
        }

        return smallCount >= MIN_STRUCTURING_COUNT && smallTotal >= MIN_STRUCTURING_TOTAL;
    }
}


@DisplayName("Excessive Reversal Pattern Tests")
class ExcessiveReversalPatternTest {

    private List<TransactionEntry> entries;
    private long now;

    @BeforeEach
    void setUp() {
        entries = new ArrayList<>();
        now = System.currentTimeMillis();
    }

    @Nested
    @DisplayName("Basic detection tests (requires 2+ qualifying deposits)")
    class BasicDetectionTests {

        @Test
        @DisplayName("Should detect reversal when 2+ deposits qualify")
        void shouldDetectReversalPattern() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 900.0, "Deposit"));

            // Credit 900 qualifies with 1000? 900 >= 900? YES
            // Credit 900 qualifies with 900? 900 >= 810? YES
            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900.0, creditTime);

            assertTrue(result, "Should detect when 2+ deposits qualify");
        }

        @Test
        @DisplayName("Should detect with exactly 90% threshold for 2 deposits")
        void shouldDetectAtExactly90Percent() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900.0, creditTime);

            assertTrue(result, "Should detect at exactly 90% threshold with 2 deposits");
        }

        @Test
        @DisplayName("Should NOT detect when only 1 deposit qualifies")
        void shouldNotDetectWithOnlyOneDeposit() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 2000.0, "Deposit"));

            // Credit 950 qualifies with 1000? 950 >= 900? YES
            // Credit 950 qualifies with 2000? 950 >= 1800? NO
            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertFalse(result, "Need at least 2 qualifying deposits");
        }

        @Test
        @DisplayName("Should NOT detect when Credit < 90% of deposits")
        void shouldNotDetectBelowThreshold() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 850.0, creditTime);

            assertFalse(result, "Should NOT detect when credit < 90%");
        }
    }

    @Nested
    @DisplayName("Time constraint tests")
    class TimeConstraintTests {

        @Test
        @DisplayName("Should require deposits within 1 minute of current credit")
        void shouldRequireDepositsWithin1Minute() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 55000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 45000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertTrue(result, "Should detect when deposits within 1 minute");
        }

        @Test
        @DisplayName("Should NOT detect when deposits more than 1 minute away")
        void shouldNotDetectWhenDepositsTooOld() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 70000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 65000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertFalse(result, "Should NOT detect when deposits > 1 minute away");
        }

        @Test
        @DisplayName("Should ignore deposits older than 5 minutes from credit")
        void shouldIgnoreOldDeposits() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 360000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 350000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertFalse(result, "Should ignore deposits older than 5 minutes");
        }

        @Test
        @DisplayName("Should work with deposits exactly 30 seconds before credit")
        void shouldWorkWith30SecondsBefore() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 30000, 900.0, "Deposit"));

            // Credit must qualify with BOTH deposits
            // For 1000: need >= 900
            // For 900: need >= 810
            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900.0, creditTime);

            assertTrue(result, "Credit 900 should qualify with both deposits");
        }
    }

    @Nested
    @DisplayName("Transaction type tests")
    class TransactionTypeTests {

        @Test
        @DisplayName("Should NOT detect for Deposit transactions")
        void shouldNotDetectForDeposit() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Deposit", 950.0, creditTime);

            assertFalse(result, "Should only detect for Credit transactions");
        }

        @Test
        @DisplayName("Should be case insensitive for Credit type")
        void shouldBeCaseInsensitive() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            boolean resultLower = isExcessiveReversalPattern(
                    entries, "credit", 950.0, creditTime);
            boolean resultUpper = isExcessiveReversalPattern(
                    entries, "CREDIT", 950.0, creditTime);

            assertTrue(resultLower, "Should detect 'credit' lowercase");
            assertTrue(resultUpper, "Should detect 'CREDIT' uppercase");
        }

        @Test
        @DisplayName("Should only count Deposit transactions (not other Credits)")
        void shouldOnlyCountDepositTransactions() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Credit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 10000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertTrue(result, "Should count only Deposit transactions (2 deposits qualify)");
        }
    }

    @Nested
    @DisplayName("Edge cases tests")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle empty entries list")
        void shouldHandleEmptyEntries() {
            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, now);

            assertFalse(result, "Should return false for empty entries");
        }

        @Test
        @DisplayName("Should handle null type in entries")
        void shouldHandleNullTypeInEntries() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, null));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 10000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 950.0, creditTime);

            assertTrue(result, "Should ignore entries with null type and still detect");
        }

        @Test
        @DisplayName("Should handle very large sums correctly")
        void shouldHandleLargeSums() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1_000_000_000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1_000_000_000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900_000_000.0, creditTime);

            assertTrue(result, "Should handle large sums correctly");
        }

        @Test
        @DisplayName("Should handle zero sum transactions")
        void shouldHandleZeroSum() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 30000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 0.0, creditTime);

            assertFalse(result, "Zero sum credit should not be >= 90% of deposits");
        }

        @Test
        @DisplayName("Real scenario: Two deposits within 1 minute, credit qualifies with both")
        void testRealScenario() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 45000, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 15000, 1000.0, "Deposit"));

            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900.0, creditTime);

            assertTrue(result, "Credit 900 should qualify with 1000 deposits (90%)");
        }

        @Test
        @DisplayName("Test with different deposit amounts - both must qualify")
        void testDifferentDepositAmounts() {
            long creditTime = now;

            entries.add(new TransactionEntry(
                    creditTime - 40000, 800.0, "Deposit"));
            entries.add(new TransactionEntry(
                    creditTime - 20000, 1000.0, "Deposit"));

            // For 800: need >= 720
            // For 1000: need >= 900
            // Credit 900 qualifies with both
            boolean result = isExcessiveReversalPattern(
                    entries, "Credit", 900.0, creditTime);

            assertTrue(result, "Credit 900 should qualify with 800 and 1000 deposits");
        }
    }

    private boolean isExcessiveReversalPattern(
            List<TransactionEntry> entries,
            String currentType,
            double currentSum,
            long currentTime) {

        if (!"Credit".equalsIgnoreCase(currentType)) {
            return false;
        }

        int reversalCount = 0;
        long REVERSAL_WINDOW_MS = 300000;
        double REVERSAL_THRESHOLD = 0.9;
        long MAX_TIME_BETWEEN_MS = 60000;
        int MIN_REVERSAL_COUNT = 2;

        for (TransactionEntry e : entries) {
            if ("Deposit".equalsIgnoreCase(e.getType())) {
                if (currentTime - e.getTimestamp() <= REVERSAL_WINDOW_MS) {
                    boolean amountMatch = currentSum >= e.getSum() * REVERSAL_THRESHOLD;
                    boolean timeMatch = (currentTime - e.getTimestamp()) <= MAX_TIME_BETWEEN_MS;

                    if (amountMatch && timeMatch) {
                        reversalCount++;
                    }
                }
            }
        }

        return reversalCount >= MIN_REVERSAL_COUNT;
    }
}