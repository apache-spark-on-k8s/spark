#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM openjdk:8-alpine


RUN apk upgrade --no-cache &&  \
    apk add --no-cache bash tini &&  \
    rm /bin/sh &&  \
    ln -sv /bin/bash /bin/sh &&  \
    chgrp root /etc/passwd  &&  \
    chmod ug+rw /etc/passwd &&  \
    mkdir -p /opt/token-refresh-server &&  \
    mkdir -p /opt/token-refresh-server/jars &&  \
    mkdir -p /opt/token-refresh-server/work-dir 

ADD target/token-refresh-server-kubernetes_2.11-2.2.0-k8s-0.5.0-SNAPSHOT-assembly.tar.gz  \
    /opt/token-refresh-server/jars
WORKDIR /opt/token-refresh-server/work-dir

# The docker build command should be invoked from the top level directory of
# the token-refresh-server project. E.g.:
# docker build -t hadoop-token-refresh-server:latest  \
#     -f src/main/docker/Dockerfile .

CMD /sbin/tini -s -- /usr/bin/java  \
    -cp $APPLICATION_CONF_DIR:$HADOOP_CONF_DIR:'/opt/token-refresh-server/jars/*'  \
    org.apache.spark.security.kubernetes.TokenRefreshServer  \
    $TOKEN_REFRESH_SERVER_ARGS
