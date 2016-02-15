#!/bin/bash

mysql_install_db && \
 mysqld && \
 mysql -e "create database druid;" && \
 mysql -e "CREATE USER 'druid'@'localhost' IDENTIFIED BY 'druid';" && \
 mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'druid'@'localhost';" && \
 /druid/run_druid_server.sh coordinator &

sleep 30

/druid/run_druid_server.sh overlord &

/druid/run_druid_server.sh historical &

/druid/run_druid_server.sh broker

