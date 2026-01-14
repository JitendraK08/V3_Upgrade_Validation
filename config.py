from configparser import ConfigParser


def load_config(file_path="config.properties"):
    config = ConfigParser()

    # ConfigParser expects sections, so we add one if missing
    with open(file_path) as f:
        file_content = "[DEFAULT]\n" + f.read()

    config.read_string(file_content)
    return config["DEFAULT"]



# a="['wrew','wdee]"
# print(list(a))
# print(a)