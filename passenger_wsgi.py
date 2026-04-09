from a2wsgi import ASGIMiddleware
from app import app as asgi_app
application = ASGIMiddleware(asgi_app)
