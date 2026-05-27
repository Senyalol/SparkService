package test;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import spark.SparkStreamingApp;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Negative M Credit Detection Tests")
class NegativeMCreditTest {

    @Nested
    @DisplayName("Credit transactions tests")
    class CreditTransactionsTests {

        @Test
        @DisplayName("Should detect negative M when credit > current M")
        void shouldDetectNegativeM() {
            double currentM = 100.0;
            String type = "Credit";
            double sum = 150.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertTrue(isNegative, "Expected negative M detection when credit > current M");
        }

        @Test
        @DisplayName("Should not detect negative M when credit <= current M")
        void shouldNotDetectNegativeMWhenCreditLessOrEqual() {
            double currentM = 100.0;
            String type = "Credit";
            double sum = 50.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertFalse(isNegative, "Should not detect negative M when credit <= current M");
        }

        @Test
        @DisplayName("Should not detect negative M when credit equals current M")
        void shouldNotDetectWhenCreditEquals() {
            double currentM = 100.0;
            String type = "Credit";
            double sum = 100.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertFalse(isNegative, "Should not detect negative M when credit equals current M");
        }
    }

    @Nested
    @DisplayName("Deposit transactions tests")
    class DepositTransactionsTests {

        @Test
        @DisplayName("Should never be negative for deposits")
        void shouldNeverBeNegativeForDeposits() {
            double currentM = 100.0;
            String type = "Deposit";
            double sum = 1000.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertFalse(isNegative, "Deposits should never be negative M");
        }
    }

    @Nested
    @DisplayName("Case insensitivity tests")
    class CaseInsensitivityTests {

        @Test
        @DisplayName("Should detect negative M for lowercase credit")
        void shouldDetectForLowercaseCredit() {
            double currentM = 100.0;
            String type = "credit";
            double sum = 150.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertTrue(isNegative, "Should detect negative M for lowercase 'credit'");
        }

        @Test
        @DisplayName("Should detect negative M for uppercase CREDIT")
        void shouldDetectForUppercaseCredit() {
            double currentM = 100.0;
            String type = "CREDIT";
            double sum = 150.0;

            boolean isNegative = SparkStreamingApp.isNegativeMCredit(currentM, type, sum);

            assertTrue(isNegative, "Should detect negative M for uppercase 'CREDIT'");
        }
    }
}