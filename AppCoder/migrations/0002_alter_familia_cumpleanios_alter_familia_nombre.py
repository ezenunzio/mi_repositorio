# Generated by Django 4.1.3 on 2022-11-12 19:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('AppCoder', '0001_initial'),
    ]

    operations = [
        migrations.AlterField(
            model_name='familia',
            name='cumpleanios',
            field=models.DateField(),
        ),
        migrations.AlterField(
            model_name='familia',
            name='nombre',
            field=models.TextField(max_length=40),
        ),
    ]
