package spark;

public enum AnomalyType {
    BIGGER_THEN_AVG_CHECK, // Транзакция больше чем средний чек в 3 раза за 5 минут
    NEGATIVE_M, //Если M <0 и поступило списание средств
    FREQUENT_TRANSACTIONS, //Только для Credit
    BIGGEST_AND_FREQUENT_CREDIT //Частые и большие списования
}