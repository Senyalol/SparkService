package test;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import spark.TransactionEntry;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static spark.AnomalyDetectionFunction.clampM;
import static spark.AnomalyDetectionFunction.segmentBalanceFromEntries;

@DisplayName("Segment Balance Calculation Tests")
class SegmentBalanceTest {

    private List<TransactionEntry> entries;

    @BeforeEach
    void setUp() {
        entries = new ArrayList<>();
    }

    @Nested
    @DisplayName("Empty entries tests")
    class EmptyEntriesTests {

        @Test
        @DisplayName("Should return 0 for empty entries")
        void shouldReturnZeroForEmptyEntries() {
            double balance = segmentBalanceFromEntries(entries);
            assertEquals(0.0, balance, 0.001, "Empty entries should return 0");
        }

        @Test
        @DisplayName("Should return 0 for null type entries")
        void shouldReturnZeroForNullTypeEntries() {
            entries.add(new TransactionEntry(1000L, 500.0, null));
            double balance = segmentBalanceFromEntries(entries);
            assertEquals(0.0, balance, 0.001, "Null type entries should return 0");
        }
    }

    @Nested
    @DisplayName("Deposit transactions tests")
    class DepositTests {

        @Test
        @DisplayName("Should add deposit amount to balance")
        void shouldAddDepositAmount() {
            entries.add(new TransactionEntry(1000L, 100.0, "Deposit"));
            entries.add(new TransactionEntry(2000L, 50.0, "Deposit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(150.0, balance, 0.001, "Balance should be sum of deposits");
        }

        @Test
        @DisplayName("Should handle single deposit correctly")
        void shouldHandleSingleDeposit() {
            entries.add(new TransactionEntry(1000L, 500.0, "Deposit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(500.0, balance, 0.001, "Single deposit should be added correctly");
        }
    }

    @Nested
    @DisplayName("Credit transactions tests")
    class CreditTests {

        @Test
        @DisplayName("Should subtract credit amount from balance")
        void shouldSubtractCreditAmount() {
            entries.add(new TransactionEntry(1000L, 200.0, "Deposit"));
            entries.add(new TransactionEntry(2000L, 50.0, "Credit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(150.0, balance, 0.001, "Credit should be subtracted from balance");
        }

        @Test
        @DisplayName("Should not go below zero for credits")
        void shouldNotGoBelowZero() {
            entries.add(new TransactionEntry(1000L, 50.0, "Deposit"));
            entries.add(new TransactionEntry(2000L, 100.0, "Credit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(0.0, balance, 0.001, "Balance should not go below zero");
        }

        @Test
        @DisplayName("Should handle multiple credits")
        void shouldHandleMultipleCredits() {
            entries.add(new TransactionEntry(1000L, 500.0, "Deposit"));
            entries.add(new TransactionEntry(2000L, 100.0, "Credit"));
            entries.add(new TransactionEntry(3000L, 50.0, "Credit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(350.0, balance, 0.001, "Multiple credits should be handled correctly");
        }
    }

    @Nested
    @DisplayName("Mixed transactions tests")
    class MixedTransactionsTests {

        @Test
        @DisplayName("Should correctly calculate complex balance")
        void shouldCalculateComplexBalance() {
            entries.add(new TransactionEntry(1000L, 1000.0, "Deposit"));
            entries.add(new TransactionEntry(2000L, 300.0, "Credit"));
            entries.add(new TransactionEntry(3000L, 200.0, "Deposit"));
            entries.add(new TransactionEntry(4000L, 500.0, "Credit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(400.0, balance, 0.001, "Complex balance calculation failed"); // 1000 - 300 + 200 - 500 = 400
        }

        @Test
        @DisplayName("Should handle case-insensitive types")
        void shouldHandleCaseInsensitiveTypes() {
            entries.add(new TransactionEntry(1000L, 500.0, "DEPOSIT"));
            entries.add(new TransactionEntry(2000L, 100.0, "credit"));
            entries.add(new TransactionEntry(3000L, 50.0, "Credit"));

            double balance = segmentBalanceFromEntries(entries);
            assertEquals(350.0, balance, 0.001, "Case-insensitive type handling failed");
        }
    }

    @Nested
    @DisplayName("ClampM function tests")
    class ClampMTests {

        @Test
        @DisplayName("Should return positive value as is")
        void shouldReturnPositiveValue() {
            assertEquals(100.0, clampM(100.0), 0.001, "Positive value should be returned as is");
            assertEquals(0.0, clampM(0.0), 0.001, "Zero should be returned as is");
        }

        @Test
        @DisplayName("Should return 0 for negative value")
        void shouldReturnZeroForNegative() {
            assertEquals(0.0, clampM(-50.0), 0.001, "Negative value should clamp to 0");
            assertEquals(0.0, clampM(-1000.0), 0.001, "Negative value should clamp to 0");
        }
    }
}