# regenerate_state_csv.py
from lib.DataReader import read_customers
from lib.DataManipulation import count_orders_state
from lib.Utils import get_spark_session

if __name__ == "__main__":
    spark = get_spark_session("LOCAL")
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    actual_results.coalesce(1).write.csv("test_result/state_aggregate.csv", mode="overwrite", header=False)
