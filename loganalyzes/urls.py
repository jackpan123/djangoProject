from django.urls import path

from . import views
app_name = 'loganalyzes'
urlpatterns = [
    path('index/', views.index, name='index'),
]