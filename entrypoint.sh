#!/bin/bash
set -e

myLogFile=/mnt/hummingbot/logs/entrypoint.log

echo "$(date): ------------------------" >> $myLogFile
echo "$(date): Entrypoint of Hummingbot" >> $myLogFile

echo "$(date): Create symbolic links" >> $myLogFile
ln -sf /mnt/hummingbot/certs /home/hummingbot
ln -sf /mnt/hummingbot/conf /home/hummingbot
ln -sf /mnt/hummingbot/data /home/hummingbot
ln -sf /mnt/hummingbot/logs /home/hummingbot

botDir=`printenv BOT_DIR`

if [[ ! -z $botDir ]]
then
  echo "$(date): 'BOT_DIR' is ${botDir}" >> $myLogFile

  for subDir in connectors controllers scripts strategies
  do
    sourceDir=/home/hummingbot/bots/$botDir/conf/$subDir
    targetDir=/mnt/hummingbot/conf

    if [[ -e $sourceDir ]]
    then
      mkdir -p $targetDir
      echo "$(date): Copy ${sourceDir} to ${targetDir}" >> $myLogFile
      cp -r $sourceDir $targetDir
    fi
  done
else
  echo "$(date): No 'BOT_DIR' environment variable set" >> $myLogFile
fi

# Execute the container's main processes
exec "$@"
