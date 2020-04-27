#!/usr/bin/bash

# Invoke from manage.py dir

# Use to load static data into a database

#python3.6 /opt/bilal/gateway_api/python/redshift2csv.py

while   getopts r: parm

do  case    $parm in

         (r)     report="$OPTARG";;


esac

done



python /opt/bilal/bilal_gateway_etl/python/redshift2csv.py -r $report

if [ $? -eq 0 ]
then
  echo "Successfully executed script"
else
  # Redirect stdout from echo command to stderr.
  echo "Script exited with error." >&2
fi
