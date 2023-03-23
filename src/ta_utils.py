from pandas import Index


class InvalidInputException(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


def validate_columns(df_columns: Index, required_columns: list[str]) -> None:
    """Validate required columns"""
    if [col for col in required_columns if col not in df_columns]:
        raise InvalidInputException(
            f"{', '.join(required_columns)} columns are required. DataFrame only included columns: {', '.join(list(df_columns))}"
        )
