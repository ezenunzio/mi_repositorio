# Generated by Django 4.1.3 on 2022-11-08 00:28

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='familia',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('nombre', models.TextField()),
                ('apellido', models.TextField()),
                ('edad', models.IntegerField()),
                ('cumpleanios', models.DateField()),
            ],
        ),
    ]
