from django.db import models

# Create your models here.
class SocketLog(models.Model):
    # def __str__(self):
    #     return self.question_text
    host_ip = models.CharField(max_length=200)
    host_port = models.IntegerField()
    username = models.CharField(max_length=200)
    password = models.CharField(max_length=200)
    log_position = models.CharField(max_length=500)
    # @admin.display(
    #     boolean=True,
    #     ordering='pub_date',
    #     description='Published recently?'
    # )
    # def was_published_recently(self):
    #     now = timezone.now()
    #     return now - datetime.timedelta(days=1) <= self.pub_date <= now