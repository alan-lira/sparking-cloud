#!/bin/bash

# MIT License

# Copyright (c) 2023 Alan Lira

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# Script Begin.

# Get Number of Provided Arguments.
number_of_provided_arguments=$#

# Set Required Arguments Array.
required_arguments_array=("Spark Version (String)",
                          "Hadoop Version (String)",
                          "Verbose Standard Output (stdout) and Standard Error (stderr) Logs [Bool]")
number_of_required_arguments=${#required_arguments_array[@]}

# Set Optional Arguments Array.
optional_arguments_array=()
number_of_optional_arguments=${#optional_arguments_array[@]}

# Parse Provided Arguments.
if [ $number_of_provided_arguments -lt $number_of_required_arguments ]; then
    if [ $number_of_required_arguments -gt 0 ]; then
        echo -e "Required Arguments ($number_of_required_arguments):"
        for i in $(seq 0 $(($number_of_required_arguments-1))); do
            echo "$(($i+1))) ${required_arguments_array[$i]}"
        done
    fi
    if [ $number_of_optional_arguments -gt 0 ]; then
        echo -e "\nOptional Arguments ($number_of_optional_arguments):"
        for i in $(seq 0 $(($number_of_optional_arguments-1))); do
            echo "$(($i+$number_of_required_arguments+1))) ${optional_arguments_array[$i]}"
        done
    fi
    exit 1
fi

# Script Arguments.
spark_version=${1}
hadoop_version=${2}
verbose_scripts=${3}

# Default 'stdout' and 'stderr' Logs Destination (Verbose Logs).
stdout_redirection="/dev/tty"
stderr_redirection="/dev/tty"

# If 'verbose_scripts' == No, Set 'stdout' and 'stderr' Logs Destination to Null (Silenced Logs).
if [ "$verbose_scripts" = False ]; then
    stdout_redirection="/dev/null"
    stderr_redirection="/dev/null"
fi

# Steps Counter.
step=0

# Updating the 'available packages' list and installing 'Scala'...
((step++))
echo -e "\n-------\n$step) Updating the 'available packages' list and installing 'Scala'..." \
1> $stdout_redirection \
2> $stderr_redirection
sudo apt-get update && \
sudo apt-get install scala -y && \
java --version \
1> $stdout_redirection \
2> $stderr_redirection

# Downloading and extracting 'Spark + Hadoop'...
((step++))
echo -e "\n-------\n$step) Downloading and extracting 'Spark (v.$spark_version) + Hadoop (v.$hadoop_version)'..." \
1> $stdout_redirection \
2> $stderr_redirection
wget -q https://archive.apache.org/dist/spark/spark-$spark_version/spark-$spark_version-bin-hadoop$hadoop_version.tgz && \
tar xvf spark-$spark_version-bin-hadoop$hadoop_version.tgz && \
rm -rf spark-*.tgz \
1> $stdout_redirection \
2> $stderr_redirection

#
sed '5 i export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' -i .bashrc

# Script End.
exit 0

