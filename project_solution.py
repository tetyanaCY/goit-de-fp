from prefect import flow

# Import flows
from landing_to_bronze import landing_to_bronze_flow
from bronze_to_silver import bronze_to_silver_flow
from silver_to_gold import silver_to_gold_flow

@flow
def main_flow():
    landing_to_bronze_flow()
    bronze_to_silver_flow()
    silver_to_gold_flow()

if __name__ == "__main__":
    main_flow()
