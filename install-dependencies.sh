#!/bin/bash
#
# This file is open source software, licensed to you under the terms
# of the Apache License, Version 2.0 (the "License").  See the NOTICE file
# distributed with this work for additional information regarding copyright
# ownership.  You may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

. /etc/os-release

if [ "$ID" = "ubuntu" ] || [ "$ID" = "debian" ]; then
    apt-get update
    apt-get install -y maven openjdk-11-jdk-headless git rpm devscripts debhelper fakeroot dpkg-dev
elif [ "$ID" = "fedora" ] || [ "$ID" = "centos" ]; then
    dnf install -y maven java-11-openjdk-headless git rpm-build devscripts debhelper fakeroot dpkg-dev
fi
