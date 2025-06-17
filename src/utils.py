import yaml


class FlowDict(dict):
    pass

def flow_dict_representer(dumper, data):
    return dumper.represent_mapping(dumper.DEFAULT_MAPPING_TAG, data, flow_style=True)

class CustomDumper(yaml.SafeDumper):
    pass

CustomDumper.add_representer(FlowDict, flow_dict_representer)
