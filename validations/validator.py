import logging

class Validator(object):
    def __init__(self, schema):
        self.schema = schema

    def check_header(self, field_name):
        schema_field_names = []
        for item in self.schema:
            schema_field_names.append(item["field_name"])

        logging.warning(f"Checking the `{field_name}` is one of possible headers: {', '.join(schema_field_names)}")

        
        return field_name in schema_field_names

    def check_type(self, field_name, field_value):
        logging.warning(f"Checking the data type for `{field_name}` where value is `{field_value}`")
        for item in self.schema:
            if field_name == item["field_name"]:
                # mapping of known data types from string representation to type()
                data_type = item["datatype"]

                if "str" in item["datatype"].lower():
                    data_type = str
                if "int" in item["datatype"].lower():
                    data_type = int
                if "dec" in item["datatype"].lower():
                    data_type = float
                if "float" in item["datatype"].lower():
                    data_type = float
                if not isinstance(field_value, data_type):
                    return False
        else:
            return True

    def apply_type(self, field_name, field_value):

        logging.warning(f"Applying correct data type for `{field_name}` where value is `{field_value}`")

        for item in self.schema:
            if field_name == item["field_name"]:
                # mapping of known data types from string representation to type()
                if "str" in item["datatype"].lower():
                    data_type = str
                if "int" in item["datatype"].lower():
                    data_type = int
                    field_value = int(field_value)
                if "dec" in item["datatype"].lower():
                    data_type = float
                    field_value = float(field_value)
                if "float" in item["datatype"].lower():
                    data_type = float
                    field_value = float(field_value)

        return field_value

    def check_record_length(self, field_name, field_value):
        logging.warning(f"Checking the record length of data value `{field_value}` for `{field_name}`.")

        for item in self.schema:
            if field_name == item["field_name"]:
                if len(field_value.strip()) > int(item["width"]):
                    return False
        else:
            return True

    def split_record(self, field_name, field_value):

        logging.warning(f"Splitting the value for `{field_name}` because `{field_value}` is too long.")

        for item in self.schema:
            if field_name == item["field_name"]:
                if len(field_value.strip()) > int(item["width"]):
                    return (
                        field_value.strip()[: int(item["width"])],
                        field_value.strip()[int(item["width"]) :],
                    )
        else:
            return None, None
