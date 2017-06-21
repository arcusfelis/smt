FROM phusion/baseimage
MAINTAINER Mikhail Uvarov <arcusfelis@gmail.com>
ENV SMT_VSN master
RUN buildDeps=' \
    git \
    unzip \
    wget \
    ' && \
    useradd -ms /bin/bash smt && \
    apt-get update && \
    apt-get install -y --no-install-recommends $buildDeps && \
# Download erlang and rebar
    wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb && \
    dpkg -i erlang-solutions_1.0_all.deb && \
    apt-get update && \
    # Save packet to the current directory
    apt-get download esl-erlang=1:18.3.4 && \
    # Do not install X11 deps
    dpkg -i --ignore-depends=libwxbase,libwxgtk --force-depends "esl-erlang_1%3a18.3.4_amd64.deb" && \
    rm "esl-erlang_1%3a18.3.4_amd64.deb" && \
    wget https://raw.github.com/wiki/rebar/rebar/rebar -O /bin/rebar && \
# download and unpack
    wget http://github.com/arcusfelis/smt/zipball/${SMT_VSN}/ -O repo.zip && \
    unzip repo.zip && \
    rm repo.zip && \
    mv arcusfelis-smt-* /smt && \
    chmod u+x /bin/rebar && \
    cd /smt && \
# create release
    /bin/rebar get-deps compile && \
    cd /smt/rel && \
    /bin/rebar generate && \
    cd / && \
# installation
    mv /smt/rel/smt /home/smt/smt && \
    chown -R smt:smt /home/smt/smt && \
# cleanup
    apt-get purge -y --auto-remove $buildDeps esl-erlang && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    rm -rf erlang-solutions_1.0_all.deb && \
    rm /bin/rebar && \
    rm -rf /smt
EXPOSE 4000
USER "smt"
CMD ["/home/smt/smt/bin/smt", "console", "-noshell",  "-noinput"]
