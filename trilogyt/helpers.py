from trilogy.core.models.datasource import Address


def safe_string_address(address: str | Address) -> str:
    if isinstance(address, Address):
        return address.location
    return address
