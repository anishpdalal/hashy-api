# Import all the models, so that Base has them before being
# imported by Alembic
from app.db.base_class import Base  # noqa
from app.models.user import OAuthAccount, User  # noqa
from app.models.sources import Source  # noqa
from app.models.documents import Document  # noqa