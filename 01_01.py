from colorama import Fore, Style
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "sasl_plain_username": "admin",
    "sasl_plain_password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Initialize KafkaAdminClient
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["sasl_plain_username"],
    sasl_plain_password=kafka_config["sasl_plain_password"],
)

my_name = "ginger"
athlete_event_results = f"{my_name}_athlete_event_results"
athlete_enriched_avg = f"{my_name}_athlete_enriched_avg"
num_partitions = 2
replication_factor = 1

# Topics
topics = [
    NewTopic(athlete_event_results, num_partitions, replication_factor),
    NewTopic(athlete_enriched_avg, num_partitions, replication_factor),
]

# Check and Delete Topics (if they exist)
existing_topics = set(admin_client.list_topics())
topics_to_delete = [athlete_event_results, athlete_enriched_avg]
topics_to_delete = [topic for topic in topics_to_delete if topic in existing_topics]

if topics_to_delete:
    try:
        delete_futures = admin_client.delete_topics(topics=topics_to_delete)
        for topic, future in delete_futures.items():
            try:
                future.result()  # Block until the delete is complete
                print(Fore.MAGENTA + f"Topic '{topic}' deleted successfully." + Style.RESET_ALL)
            except Exception as e:
                print(Fore.MAGENTA + f"Failed to delete topic '{topic}': {e}" + Style.RESET_ALL)
    except Exception as e:
        print(Fore.MAGENTA + f"An error occurred during topic deletion: {e}" + Style.RESET_ALL)
else:
    print(Fore.YELLOW + "No topics to delete." + Style.RESET_ALL)

# Create Topics
try:
    create_futures = admin_client.create_topics(topics)
    for topic, future in create_futures.items():
        try:
            future.result()  # Block until the create is complete
            print(Fore.GREEN + f"Topic '{topic.name}' created successfully." + Style.RESET_ALL)
        except TopicAlreadyExistsError:
            print(Fore.BLUE + f"Topic '{topic.name}' already exists." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.MAGENTA + f"Failed to create topic '{topic.name}': {e}" + Style.RESET_ALL)
except Exception as e:
    print(Fore.MAGENTA + f"An error occurred during topic creation: {e}" + Style.RESET_ALL)

# List Existing Topics
try:
    metadata = admin_client.list_topics()
    for topic in metadata:
        if my_name in topic:
            print(Fore.BLUE + f"Topic '{topic}' already exists." + Style.RESET_ALL)
except Exception as e:
    print(Fore.MAGENTA + f"Failed to list topics: {e}" + Style.RESET_ALL)
finally:
    print("Closing Kafka Admin Client...")
    admin_client.close()
    print("Kafka Admin Client closed successfully.")
