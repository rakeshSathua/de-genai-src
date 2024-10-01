from GPTBigQueryInterface import GPTBigQueryInterface


credentials_path = "path_to_your_service_account_json.json"
project_id = "ds-webdev"
dataset_id = "de_genai_logging"

interface = GPTBigQueryInterface(project_id, user_dataset=dataset_id)
while True:
    input_text = input("You : ")  # Example input
    if input_text == "exit":
        print("end of the Chat")
        break

    interface.run(input_text)
