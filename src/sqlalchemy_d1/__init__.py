"""SQLAlchemy dialect for Cloudflare D1"""

from .dialect import D1Dialect
from sqlalchemy.dialects import registry

registry.register("d1", "sqlalchemy_d1.dialect", "D1Dialect")
