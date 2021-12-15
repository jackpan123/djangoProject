from django.contrib import admin

# Register your models here.
from django.contrib import admin

from .models import SocketLog

class SocketLogAdmin(admin.ModelAdmin):
    list_display = ('host_ip', 'host_port', 'username', "password", "log_position")


admin.site.register(SocketLog, SocketLogAdmin)