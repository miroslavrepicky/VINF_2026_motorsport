FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jdk ant build-essential python3-dev python3-pip python3-venv curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV JCC_JDK=$JAVA_HOME
RUN python3 -m venv /venv && /venv/bin/pip install -U pip setuptools
ENV PATH=/venv/bin:$PATH

# JCC hľadá libjava/libjvm v starom umiestnení jre/lib/amd64, v Jave 21 sú o úroveň vyššie
RUN mkdir -p $JAVA_HOME/jre/lib/amd64 && \
    ln -s $JAVA_HOME/lib/libjava.so $JAVA_HOME/jre/lib/amd64/libjava.so && \
    ln -s $JAVA_HOME/lib/server $JAVA_HOME/jre/lib/amd64/server

RUN curl -fsSL https://archive.apache.org/dist/lucene/pylucene/pylucene-10.0.0-src.tar.gz | tar xz
RUN cd pylucene-10.0.0/jcc && python setup.py build && python setup.py install
RUN cd pylucene-10.0.0 && \
    make PREFIX_PYTHON=/venv ANT=ant PYTHON=python JCC="python -m jcc" NUM_FILES=10 && \
    make install PREFIX_PYTHON=/venv ANT=ant PYTHON=python JCC="python -m jcc" NUM_FILES=10
WORKDIR /work