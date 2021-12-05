from django.urls import path

from . import views
app_name = 'loganalyzes'
urlpatterns = [
    path('index/', views.index, name='index'),
    path('upload_file/', views.upload_file, name='upload_file'),
]