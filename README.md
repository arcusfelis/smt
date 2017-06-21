smt
===

simple mysql timer :)

This simple application exports metrics from MySQL into Graphite.


docker build
============

```bash
docker build . -t smt
```


docker example
============

```bash
# run mysql which you want to monitor
docker run --name example-mysql \
    -p 33061:3306 \
    --health-cmd='mysqladmin ping --silent' \
    -e MYSQL_USER=ejabberd \
    -e MYSQL_DATABASE=ejabberd \
    -e MYSQL_PASSWORD=ejabberd \
    -e MYSQL_ROOT_PASSWORD=ejabberd \
    -d mysql:5.7

# run graphite
docker run -d --name graphite_example -v /data/graphite:/data \
           -e SECRET_KEY='secret' \
           -p 8080:80 -p 3000:3000 -p 2003:2003 -p 2004:2004 -p 7002:7002 \
           -p 8125:8125/udp -p 8126:8126 samsaffron/graphite


# Run smt
# WARNING! don't just --link graphite
docker run \
  --link graphite_example:grapitehost \
  --link example-mysql \
  -e GRAPHITE_HOST=grapitehost \
  -e GRAPHITE_URL=http://grapitehost \
  -e MYSQL_POOLS=mysql1 \
  -e MYSQL_MYSQL1_HOST=example-mysql \
  -e MYSQL_MYSQL1_USER=ejabberd \
  -e MYSQL_MYSQL1_DATABASE=ejabberd \
  -e MYSQL_MYSQL1_PASSWORD=ejabberd \
  -d --name example-smt arcusfelis/smt:v0.1

# Open graphite
xdg-open 127.0.0.1:8080
```
