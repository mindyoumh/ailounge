from django.urls import path
from .views import process_ticket

urlpatterns = [
    path("process_tickets", process_ticket, name="process_ticket"),
]
