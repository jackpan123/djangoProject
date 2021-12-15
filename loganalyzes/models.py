from django.db import models

# Create your models here.
from django.utils.html import format_html
from django.contrib import admin


class SocketLog(models.Model):
    def __str__(self):
        return self.host_ip
    host_ip = models.CharField(max_length=200)
    host_port = models.IntegerField()
    username = models.CharField(max_length=200)
    password = models.CharField(max_length=200)
    log_position = models.CharField(max_length=500)
    # @admin.display(
    #     boolean=True,os
    #     ordering='pub_date',
    #     description='Published recently?'
    # )

    @admin.display
    def start_monitor(self):
        return format_html(
            '<a href="/loganalyzes/{}/start_monitor/">Start monitor</a>',
            self.pk
        )
    # def was_published_recently(self):
    #     now = timezone.now()
    #     return now - datetime.timedelta(days=1) <= self.pub_date <= now