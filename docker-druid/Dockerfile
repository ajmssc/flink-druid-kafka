FROM anapsix/alpine-java


VOLUME ["/var/lib/mysql", "/etc/mysql/conf.d/"]

RUN apk add --update sudo mysql mysql-client && \
    mkdir -p /run/mysqld/ && \
    mkdir -p /var/lib/mysql && \
    mkdir -p /etc/mysql/conf.d && \
    { \
        echo '[mysqld]'; \
        echo 'user = root'; \
        echo 'datadir = /var/lib/mysql'; \
        echo 'port = 3306'; \
        echo 'socket = /run/mysqld/mysqld.sock'; \
        echo 'log-bin = /var/lib/mysql/mysql-bin'; \
        echo 'bind-address = 127.0.0.1'; \
        echo '!includedir /etc/mysql/conf.d/'; \
    } > /etc/mysql/my.cnf


ENV TERM xterm


RUN wget http://static.druid.io/artifacts/releases/druid-0.8.3-bin.tar.gz -O /druid.tar.gz\
    && tar xvzf /druid.tar.gz -C / && rm -rf /druid.tar.gz && mv /druid* /druid

ADD common.runtime.properties /druid/config/_common/common.runtime.properties

ADD start.sh /usr/bin/start.sh

CMD ["start.sh"]
