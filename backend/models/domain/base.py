from sqlalchemy.orm import DeclarativeBase


class DomainBase(DeclarativeBase):
    pass


# Base alias for convenient unified usage
Base = DomainBase
