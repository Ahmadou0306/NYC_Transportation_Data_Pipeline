PROJECT_DATA_COLLECTOR_NAME = "NYC_TRANSPORTATION_DATA_COLLECT"


def get_collected_tags(collected_name:str, frequency:str="days"):
    
    all_collected_tags = {
        "hour": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:hourly",
            "schedule:every-hour",
            "offset-1",
            "retention:7d",
            "priority:normal"
        ],
        "days": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:daily",
            "schedule:daily",
            "offset-1",
            "retention:90d",
            "priority:high"
        ],
        "weekly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:weekly",
            "schedule:weekly",
            "offset-1",
            "retention:1y",
            "priority:low"
        ],
        "monthly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:monthly",
            "schedule:monthly",
            "offset-1",
            "retention:3y",
            "priority:high"
        ],
        "yearly": [
            PROJECT_DATA_COLLECTOR_NAME, 
            "frequency:yearly",
            "schedule:yearly",
            "offset-1",
            "retention:10y",
            "priority:low"
        ],
    }

    if frequency not in all_collected_tags.keys():
        raise ValueError(
                    f"Fréquence '{frequency}' invalide. "
                    f"Valeurs acceptées: {list(all_collected_tags.keys())}"
                )    
    return all_collected_tags[frequency]+[collected_name]

def push_x_com(ti, message, value):
    """Pousse un message dans XCom"""
    ti.xcom_push(key=message, value=value)
    print("Message envoyé dans XCom")


def pull_x_com(ti, task_origins, key):
    """Récupère le message depuis XCom"""
    message = ti.xcom_pull(task_ids=task_origins, key=key)
    print(f"Message reçu : {message}")