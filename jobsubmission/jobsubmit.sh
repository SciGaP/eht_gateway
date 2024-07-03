#!/bin/bash

# Check if the user provided an argument
if [ -z "$1" ]; then
  echo "Usage: $0 <directory_name>"
  exit 1
fi

# Create the directory
mkdir -p jobs/"$1"

# cp job template folder
cp -pr job_template jobs/"$1"/job

cd jobs/"$1"/job

# modify submit script
# in bin/submit, replace batch_name = as batch_name = $1

new_var="\"$1\""
#echo "$new_var"

# use double quote for variable
sed -i "s/batch_name =/batch_name = $new_var/g" bin/submit

# submit job
bin/batches