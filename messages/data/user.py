import logging

logging.basicConfig(level=logging.INFO)


class User:
    """
    The User class represents a user object.
    It contains information about the user, such as the user ID, name, age, and email.
    """

    def __init__(self, data: tuple) -> None:
        """Initializes the User object."""
        self._name = data["name"]
        self._user_id = data["user_id"]
        self._first_name = data["first_name"]
        self._last_name = data["last_name"]
        self._age = data["age"]
        self._email = data["email"]
        self._logger = logging.getLogger(__name__)

    def user_to_dictionary(self) -> dict:
        """Converts the User object to a dictionary."""
        return {
            "name": self._name,
            "user_id": self._user_id,
            "first_name": self._first_name,
            "last_name": self._last_name,
            "age": self._age,
            "email": self._email,
        }

    def delivery_confirmation(self, err: str, message: str) -> None:
        """Logs the delivery confirmation of the message."""
        if err is not None:
            self._logger.error(f"Failed to deliver message: {err}")
        else:
            self._logger.info(f"Message delivered: {message}")
