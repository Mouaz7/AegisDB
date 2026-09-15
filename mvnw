#!/bin/sh
# ----------------------------------------------------------------------------
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# ----------------------------------------------------------------------------

# ----------------------------------------------------------------------------
# Maven Start Up Batch script
#
# Required ENV vars:
# ------------------
#   JAVA_HOME - location of a JDK home dir
#
# Optional ENV vars
# -----------------
#   M2_HOME - location of maven2's installed home dir
#   MAVEN_OPTS - parameters to passed to the Java VM when running Maven
#     e.g. to debug Maven itself, use
#       set MAVEN_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000
#   MAVEN_SKIP_RC - flag to disable loading of mavenrc files
# ----------------------------------------------------------------------------

if [ -z "$MAVEN_SKIP_RC" ]; then
  if [ -f /etc/mavenrc ]; then
    . /etc/mavenrc
  fi
  if [ -f "$HOME/.mavenrc" ]; then
    . "$HOME/.mavenrc"
  fi
fi

# OS specific support.  $var _must_ be set to either true or false.
cygwin=false;
darwin=false;
mingw=false
case "`uname`" in
  CYGWIN*) cygwin=true ;;
  Darwin*) darwin=true ;;
  MINGW*) mingw=true ;;
esac

## resolve links - $0 may be a link to the script
PRG="$0"

# need this for relative symlinks
while [ -h "$PRG" ] ; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG="`dirname "$PRG"`/$link"
  fi
done

saveddir=`pwd`

M2_HOME=`dirname "$PRG"`

# make it fully qualified
M2_HOME=`cd "$M2_HOME" && pwd`

cd "$saveddir"

# Determine the Java command to use to start the JVM.
if [ -n "$JAVA_HOME" ] ; then
  if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
    # IBM's JDK on AIX uses strange locations for the executables
    JAVACMD="$JAVA_HOME/jre/sh/java"
  else
    JAVACMD="$JAVA_HOME/bin/java"
  fi
  if [ ! -x "$JAVACMD" ] ; then
    echo "The JAVA_HOME environment variable is not defined correctly" >&2
    echo "This environment variable is needed to run this program" >&2
    echo "NB: JAVA_HOME should point to a JDK not a JRE" >&2
    exit 1
  fi
else
  JAVACMD="java"
  which java >/dev/null 2>&1 || (echo "JAVA_HOME is not set and no 'java' command could be found in your PATH." >&2 && exit 1)
fi

# Locate the maven wrapper jar or system maven
WRAPPER_DIR="$M2_HOME/.mvn/wrapper"
WRAPPER_JAR="$WRAPPER_DIR/maven-wrapper.jar"
WRAPPER_PROPERTIES="$WRAPPER_DIR/maven-wrapper.properties"

# If system mvn is installed and wrapper jar doesn't exist, we can fallback or download
if [ ! -f "$WRAPPER_JAR" ]; then
    if which mvn >/dev/null 2>&1; then
        exec mvn "$@"
    fi
    # Download wrapper jar or maven distribution directly
    DIST_URL=$(grep '^distributionUrl=' "$WRAPPER_PROPERTIES" | cut -d'=' -f2-)
    MAVEN_USER_HOME="${MAVEN_USER_HOME:-$HOME/.m2}"
    MVN_DIR="$MAVEN_USER_HOME/wrapper/dists/apache-maven-3.9.6"
    if [ ! -d "$MVN_DIR" ]; then
        mkdir -p "$MVN_DIR"
        echo "Downloading Maven distribution from $DIST_URL..."
        TMP_ZIP="$MVN_DIR/maven.zip"
        if which curl >/dev/null 2>&1; then
            curl -fsSL "$DIST_URL" -o "$TMP_ZIP"
        elif which wget >/dev/null 2>&1; then
            wget -q "$DIST_URL" -O "$TMP_ZIP"
        fi
        unzip -qo "$TMP_ZIP" -d "$MVN_DIR"
        rm -f "$TMP_ZIP"
    fi
    MVN_BIN=$(find "$MVN_DIR" -name "mvn" -type f | head -n 1)
    if [ -x "$MVN_BIN" ]; then
        exec "$MVN_BIN" "$@"
    fi
fi

exec "$JAVACMD" \
  $MAVEN_OPTS \
  -classpath "$WRAPPER_JAR" \
  "-Dmaven.home=$M2_HOME" \
  "-Dmaven.multiModuleProjectDirectory=$M2_HOME" \
  org.apache.maven.wrapper.MavenWrapperMain \
  "$@"
