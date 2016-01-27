# akvo-cartodb-consumer
CartoDB consumer

# Docker
```shell
$ docker -it -v (pwd):/app clojure /bin/bash
```


# Non working notes
This is not working since the ownership / permissions on the /root/.ssh is the external user. Not sure there is a way to change user on a volume...

```shell
$ docker run -it -v (pwd):/app -v (pwd)/../akvo-cartodb-consumer-ssh:/root/.ssh clojure /bin/bash
root@..:/tmp# cd app
root@..:/app# lein run /repos local.dev
...
Username for github..

yikes

root@...:/app# ssh -v github.com
OpenSSH_6.7p1 Debian-5, OpenSSL 1.0.1k 8 Jan 2015
Bad owner or permissions on /root/.ssh/config
root@...:/app# cd /root
root@...:~# ls -la
root@...:~# ls -la | grep .ssh
drwxr-xr-x  1 1000 staff  170 Jan 26 14:10 .ssh
root@...:~# 
```

