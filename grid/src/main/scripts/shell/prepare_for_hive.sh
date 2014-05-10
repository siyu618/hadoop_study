#!/bin/sh

#
# please refer to http://twiki.corp.yahoo.com/view/CCDI/UseHiveAction
#
prepare_cmd="cp"

hive_home=/home/y/libexec/hive
hive_lib=${hive_home}/lib
hive_auxblib=${hive_home}/auxlib
hive_conf=${hive_home}/conf

hive_conf_site=hive-site.xml
hive_lib_jars="hive-common.jar, hive-builtins.jar, hive-metastore.jar, hive-serde.jar, hive-exec.jar, hcatalog-core.jar, libfb303.jar, webhcat-java-client.jar, hive-service.jar, hive-cli.jar "
hive_auxlib_jars="antlr-runtime-*.jar, datanucleus-core-*.jar, jdo2-api-*.jar, jline-*.jar, derby-*.jar "


if [ $# -lt 1 ]; then
	echo "usage $0 <dest_path>"
	exit -1
fi

dest_path=$1
dest_lib_path=${dest_path}/lib

# prepare the hive_config
mkdir -p ${dest_lib_path}
$prepare_cmd ${hive_conf}/${hive_conf_site} ${dest_path}

# prepare the hive_lib_jars
for jar_file in `echo "${hive_lib_jars}" | awk -F"," 'function trim(str) { sub("^[ \t]*", "", str);  sub("[ \t]*$", "", str); return str; } {for (i=1;i <=NF; i ++) print trim($i) }' `
do
	$prepare_cmd ${hive_lib}/${jar_file} ${dest_lib_path}
done

# prepare the hive_auxlib_jars
for jar_file in `echo "${hive_auxlib_jars}" | awk -F"," 'function trim(str) { sub("^[ \t]*", "", str);  sub("[ \t]*$", "", str); return str; } {for (i=1;i <=NF; i ++) print trim($i) }' `
do
	$prepare_cmd ${hive_auxblib}/${jar_file} ${dest_lib_path}
done
