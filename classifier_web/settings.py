import os
from pathlib import Path

from adsputils import load_config

proj_home = os.path.realpath(os.path.join(os.path.dirname(__file__), "../"))
CONFIG = load_config(proj_home=proj_home)
BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = CONFIG.get("SECRET_KEY", "django-insecure-classifier-db-ui-local")
DEBUG = CONFIG.get("DEBUG", True)
ALLOWED_HOSTS = CONFIG.get("ALLOWED_HOSTS", ["*"])

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.staticfiles",
    "inspector",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.middleware.common.CommonMiddleware",
]

ROOT_URLCONF = "classifier_web.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [],
        },
    }
]

WSGI_APPLICATION = "classifier_web.wsgi.application"
ASGI_APPLICATION = "classifier_web.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

STATIC_URL = "static/"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
